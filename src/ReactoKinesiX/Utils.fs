namespace ReactoKinesix.Utils

open System
open System.Collections.Generic
open System.Globalization
open System.IO
open System.Reactive.Linq
open System.Runtime.Serialization.Json
open System.Text
open System.Threading
open System.Threading.Tasks

open log4net

open Amazon.CloudWatch
open Amazon.CloudWatch.Model
open Amazon.DynamoDBv2
open Amazon.DynamoDBv2.Model
open Amazon.Kinesis
open Amazon.Kinesis.Model

open ReactoKinesix.Model

/// Type alias for F# mailbox processor type
type Agent<'T> = MailboxProcessor<'T>

[<AutoOpen>]
module internal Utils =
    let validateConfig (config : ReactoKinesixConfig) =
        if config.HeartbeatTimeout < config.Heartbeat then
            raise <| InvalidHeartbeatConfigurationException(config.Heartbeat, config.HeartbeatTimeout)

        if config.MaxDynamoDBRetries < 0 then
            raise <| NegativeMaxDynamoDBRetriesConfigurationException(config.MaxDynamoDBRetries)

        if config.MaxKinesisRetries < 0 then
            raise <| NegativeMaxKinesisRetriesConfigurationException(config.MaxKinesisRetries)

        // work out the minimum expiry we should allow the handover requests to complete based on the 
        // other configurations, whilst also allowing extra time in case the processing of a batch 
        // takes a long time (hence delaying how quickly the current node stops heartbeating from the 
        // moment it's notified about the handover request)
        let minHandoverReqExpiry = config.HeartbeatTimeout + 
                                   config.CheckPendingHandoverRequestFrequency + 
                                   config.CheckUnprocessedShardsFrequency + 
                                   TimeSpan.FromMinutes(3.0)
        if config.HandoverRequestExpiry < minHandoverReqExpiry then
            raise <| InsufficientHandoverRequestExpiryException(config.HandoverRequestExpiry)

    let logDebug (logger : ILog) format (args : obj[]) = if logger.IsDebugEnabled then logger.DebugFormat(format, args)
    let logInfo  (logger : ILog) format (args : obj[]) = if logger.IsInfoEnabled  then logger.InfoFormat(format, args)
    let logWarn  (logger : ILog) exn format (args : obj[]) = 
        if logger.IsWarnEnabled  then 
            let msg = String.Format(format, args)
            logger.Warn(msg, exn)
    let logError (logger : ILog) exn format (args : obj[]) = 
        if logger.IsErrorEnabled then
            let msg = String.Format(format, args)
            logger.Error(msg, exn)

    let inline (!>) (b : ^b) : ^a = (^a : (static member op_Explicit : ^b -> ^a) (b)) 

    let withRetry n f =
        let rec loop n = try f() with | _ -> if n = 0 then reraise() else loop (n - 1)

        loop n

    /// Default function for calcuating delay (in milliseconds) between retries, based on (http://en.wikipedia.org/wiki/Exponential_backoff)
    /// After 8 retries the delay starts to become unreasonable for most scenarios, so cap the delay at that
    let private exponentialDelay =
        let calcDelay retries = 
            let rec sum acc = function | 0 -> acc | n -> sum (acc + n) (n - 1)

            let n = pown 2 retries - 1
            let slots = float (sum 0 n) / float (n + 1)
            int (100.0 * slots)

        let delays = [| 0..8 |] |> Array.map calcDelay

        (fun retries -> delays.[min retries 8])

    let inline csv (arr : 'a seq) = String.Join(",", arr)

    let deserialize<'T> = 
        let serializer = new DataContractJsonSerializer(typeof<'T>)
        (fun (str : string) -> 
            use memStream = new MemoryStream(Encoding.UTF8.GetBytes(str))
            serializer.ReadObject(memStream) :?> 'T)

    let serialize<'T> =
        let serializer = new DataContractJsonSerializer(typeof<'T>)
        (fun (x : 'T) -> 
            use memStream = new MemoryStream()
            serializer.WriteObject(memStream, x)
            memStream.ToArray() |> Encoding.UTF8.GetString)
    
    // since the async methods from the AWSSDK excepts with AggregateException which is not all that useful, hence
    // this active pattern which unwraps any AggregateException
    let rec flattenExn (exn : Exception) =
        match exn with
        | :? AggregateException as aggrExn -> flattenExn aggrExn.InnerException
        | exn -> exn

    let (|Flatten|) exn = flattenExn exn
    
    /// Applies memoization to the supplied function f
    let memoize (f : 'a -> 'b) =
        let cache = new Dictionary<'a, 'b>()

        let memoizedFunc (input : 'a) =
            // check if there is a cached result for this input
            match cache.TryGetValue(input) with
            | true, x   -> x
            | false, _  ->
                // evaluate and add result to cache
                let result = f input
                cache.Add(input, result)
                result

        // return the memoized version of f
        memoizedFunc

    type Observable with
        /// Returns an IObservable<T> from an Async<T> (from http://cs.hubfs.net/topic/None/59632#comment-72865)
        static member FromAsync (computation) =
            Observable.Create<'a>(Func<IObserver<'a>, IDisposable>(fun observer ->
                if observer = null then nullArg "observer"

                let cts = new System.Threading.CancellationTokenSource()

                // CancellationTokenSource can hold WaitHandle inside so it should definitly be disposed
                // use 'invoked' flag to make sure that method (either Cancel or Dispose) will be called only once
                let invoked = ref 0
                let cancelOrDispose cancel =
                    if System.Threading.Interlocked.CompareExchange(invoked, 1, 0) = 0 then
                        if cancel then cts.Cancel() else cts.Dispose()

                let wrapper = async {
                    try
                        try
                            let! result = computation
                            observer.OnNext(result)
                            observer.OnCompleted()
                        with 
                        | ex -> observer.OnError(ex)
                    finally cancelOrDispose false }

                Async.StartImmediate(wrapper, cts.Token)

                { new System.IDisposable with member this.Dispose() = cancelOrDispose(true) }))

    type Async with
        /// Retries the async computation up to specified number of times. Optionally accepts a function to calculate
        /// the delay in milliseconds between retries, default is exponential delay with a backoff slot of 500ms.
        static member WithRetry (computation : Async<'a>, maxRetries, ?calcDelay) =
            let calcDelay = defaultArg calcDelay exponentialDelay

            let rec loop retryCount =
                async {
                    let! res = computation |> Async.Catch
                    match res with
                    | Choice1Of2 x -> return Success x
                    | Choice2Of2 _ when retryCount < maxRetries -> 
                        do! calcDelay retryCount |> Async.Sleep
                        return! loop (retryCount + 1)
                    | Choice2Of2 (Flatten exn) -> return Failure exn
                }
            loop 0

        /// Starts a computation as a plain task.
        static member StartAsPlainTask (work : Async<unit>) = 
            Task.Factory.StartNew(fun () -> work |> Async.RunSynchronously)

    type MailboxProcessor<'T> with
        /// Start an agent and protect it from unhandled exceptions
        static member StartProtected (body : MailboxProcessor<_> -> Async<unit>, 
                                      ?cancellationToken : CancellationToken,
                                      ?onRestart : Exception -> unit) = 
            let onRestart = defaultArg onRestart (fun _ -> ())
            let rec watchdog f x = async {
                let! res = f x |> Async.Catch
                match res with
                | Choice1Of2 _ -> return ()
                | Choice2Of2 exn -> 
                    onRestart(exn)
                    return! watchdog f x
            }

            Agent.Start((fun inbox -> watchdog body inbox), ?cancellationToken = cancellationToken)

    module Seq =
        // originaly from http://fssnip.net/1o
        let groupsOfAtMost (size: int) (s: seq<'v>) =
            seq {
                let en = s.GetEnumerator ()
                let more = ref true
                while !more do
                    let group =
                        [|
                            let i = ref 0
                            while !i < size && en.MoveNext () do
                                yield en.Current
                                i := !i + 1
                        |]
                    if group.Length = 0 
                    then more := false
                    else yield group
            }

module internal CloudWatchUtils =
    let private logger   = LogManager.GetLogger("CloudWatchUtils")
    let private logDebug = logDebug logger
    let private logWarn  = logWarn logger

    // batch up metrics into groups of 20
    let private batchSize = 20

    /// Push a bunch of metrics
    let pushMetrics (cloudWatch : IAmazonCloudWatch) ns (metrics : Metric[]) =
        let groups   = metrics |> Seq.groupsOfAtMost batchSize |> Seq.toArray
        let requests = groups  |> Array.map (fun metrics -> 
            let req  = new PutMetricDataRequest(Namespace = ns)
            let data = metrics |> Seq.map (fun m -> 
                        let stats = new StatisticSet(Minimum = m.Min, Maximum = m.Max, Sum = m.Sum, SampleCount = m.Count)
                        let datum = new MetricDatum(MetricName      = m.MetricName, 
                                                    Timestamp       = m.Timestamp,
                                                    Unit            = m.Unit,
                                                    StatisticValues = stats)
                        datum.Dimensions.AddRange(m.Dimensions)
                        datum)
            req.MetricData.AddRange(data)
            req)

        let pushMetricsInternal (cloudWatch : IAmazonCloudWatch) req =
            async {
                let! res = Async.WithRetry(cloudWatch.PutMetricDataAsync(req) |> Async.AwaitTask, 1)
                match res with
                | Success _   -> logDebug "Successfully pushed [{0}] metrics." [| req.MetricData.Count |]
                | Failure exn -> logWarn exn "Failed to push [{0}] metrics.\n{1}" [| req.MetricData.Count; exn |]
            }

        async {
            logDebug "Pushing [{0}] metrics in [{1}] batches." [| metrics.Length; requests.Length |]
            // DON'T push metrics in parallel, avoids spikes in CPU/thread/bandwidth usage
            for req in requests do
                do! pushMetricsInternal cloudWatch req
        }

module internal KinesisUtils =
    let private logger   = LogManager.GetLogger("KinesisUtils")
    let private logDebug = logDebug logger
    let private logError = logError logger
    
    /// Returns the shards that are part of the stream
    let getShards (kinesis : IAmazonKinesis) (StreamName streamName) =
        let rec describeStream acc startShardId = 
            async {
                let req = new DescribeStreamRequest(StreamName = streamName)
                match startShardId with
                | Some shardId -> req.ExclusiveStartShardId <- shardId
                | _ -> ()
                    
                let! res = Async.WithRetry(kinesis.DescribeStreamAsync(req) |> Async.AwaitTask, 2)
                match res with 
                | Success res when not res.StreamDescription.HasMoreShards -> 
                    let shards   = (res.StreamDescription.Shards :: acc) |> Seq.collect id |> Seq.toArray

                    logDebug "Stream [{0}] has [{1}] shards" [| streamName; shards.Length |]
                    return Success shards
                | Success res ->
                    let lastShard = res.StreamDescription.Shards |> Seq.last
                    return! describeStream (res.StreamDescription.Shards :: acc) (Some lastShard.ShardId)
                | Failure (Flatten exn) -> 
                    logError exn "Failed to get shards for stream [{0}]" [| streamName |]
                    return Failure exn
            }
            
        async { return! describeStream [] None }

    /// Returns the shard iterator for the specified shard in the stream
    let getShardIterator (kinesis : IAmazonKinesis) 
                         (StreamName streamName) 
                         (ShardId shardId) 
                         iteratorType = 
        async {
            let req = GetShardIteratorRequest(StreamName = streamName, ShardId = shardId)

            match iteratorType with
            | TrimHorizon -> 
                req.ShardIteratorType       <- ShardIteratorType.TRIM_HORIZON
            | AtSequenceNumber (SequenceNumber seqNum) -> 
                req.StartingSequenceNumber  <- seqNum
                req.ShardIteratorType       <- ShardIteratorType.AT_SEQUENCE_NUMBER
            | AfterSequenceNumber (SequenceNumber seqNum) -> 
                req.StartingSequenceNumber  <- seqNum
                req.ShardIteratorType       <- ShardIteratorType.AFTER_SEQUENCE_NUMBER
            | Latest ->
                req.ShardIteratorType       <- ShardIteratorType.LATEST

            let! res = kinesis.GetShardIteratorAsync(req) |> Async.AwaitTask

            logDebug "Received shard iterator [{0}], type [{1}] for stream [{2}], shard [{3}]"
                     [| res.ShardIterator; req.ShardIteratorType; streamName; shardId |]

            return res.ShardIterator
        }

    /// Returns a number of records from the shard in the stream along with the next shard iterator
    let getRecords (kinesis : IAmazonKinesis) 
                   (config : ReactoKinesixConfig) 
                   streamName 
                   shardId
                   iterator
                   batchSize = 
        async {
            match iterator with
            | EndOfShard -> return Failure(ShardCannotBeIteratedException)
            | _ ->
                let req = GetRecordsRequest(Limit = batchSize)

                match iterator with  
                | IteratorToken(token) -> req.ShardIterator <- token
                | NoIteratorToken(iteratorType) ->
                    let! token = getShardIterator kinesis streamName shardId iteratorType
                    req.ShardIterator <- token

                let! getRecordResult = 
                    Async.WithRetry(
                        kinesis.GetRecordsAsync(req) |> Async.AwaitTask, 
                        config.MaxKinesisRetries)
                match getRecordResult with
                | Success res -> 
                    logDebug "Received [{0}] records from stream [{1}], shard [{2}]"
                             [| res.Records.Count; streamName; shardId |]
                    let records = res.Records 
                                  |> Seq.map (fun r -> !> r : Record) 
                                  |> Seq.toArray
                    let behind = TimeSpan.FromMilliseconds <| float res.MillisBehindLatest

                    return Success(behind, res.NextShardIterator, records)
                | Failure exn ->
                    logError exn "Failed to get records from stream [{0}], shard [{1}]" [| streamName; shardId |]
                    return Failure(exn) 
        }

module internal DynamoDBUtils =
    let private shardIdAttr       = "ShardId"
    let private lastHeartbeatAttr = "LastHeartbeat"
    let private workerIdAttr      = "WorkerId"
    let private checkpointAttr    = "SequenceNumberCheckpoint"
    let private checkpointAtAttr  = "CheckpointAt"
    let private timeBehindAttr    = "MillisecondsBehindLatest"
    let private isClosedAttr      = "IsClosed"
    let private handoverReqAttr   = "HandoverRequest"

    let private logger   = LogManager.GetLogger("KinesisUtils")
    let private logDebug = logDebug logger
    let private logWarn  = logWarn  logger
    let private logError = logError logger

    let private dateTimeFormat      = "yyyy-MM-dd HH:mm:ss.fffffff"
    let private currentTimestamp () = DateTime.UtcNow.ToString(dateTimeFormat)
    let private fromTimestamp str   = 
        DateTime.ParseExact(
            str, 
            dateTimeFormat, 
            CultureInfo.InvariantCulture)

    let private tryGetAttributeValue (dict : IDictionary<_, AttributeValue>) key = 
        match dict.TryGetValue key with | true, x -> Some x.S | _ -> None

    let private parseBool (str : string) = 
        match bool.TryParse(str) with 
        | true, x -> x 
        | _ -> false

    let private serializeHandoverReq   = serialize<HandoverRequest>
    let private deserializeHandoverReq = deserialize<HandoverRequest>
    
    let private (|NoShard|ClosedShard|Shard|) (item : Dictionary<string, AttributeValue>) =        
        if item = null || item.Count = 0 then NoShard
        else 
            // the shard creation should always ensure that worker ID and heartbeat is created
            // but the checkpoint is only set the first time we were able to get records from
            // the stream and processed them
            let workerId, heartbeat, checkpoint, isClosed, handoverReq = 
                item.[workerIdAttr].S,
                item.[lastHeartbeatAttr].S,
                tryGetAttributeValue item checkpointAttr,
                tryGetAttributeValue item isClosedAttr,
                tryGetAttributeValue item handoverReqAttr
                |> function | Some str -> Some <| deserializeHandoverReq str
                            | _ -> None

            match isClosed with
            | Some boolStr when parseBool boolStr -> ClosedShard
            | _ -> Shard(WorkerId workerId, 
                         fromTimestamp heartbeat, 
                         checkpoint, 
                         handoverReq)

    /// Returns the list of tables that currently exist in DynamoDB
    let private doesTableExist (dynamoDB : IAmazonDynamoDB) (TableName tableName) =
        try
            let req  = new DescribeTableRequest(TableName = tableName)        
            let res = dynamoDB.DescribeTable(req)

            logDebug "Table [{0}] current status [{1}], read throughput [{2}], write throughput [{3}]" 
                     [| tableName; res.Table.TableStatus; 
                        res.Table.ProvisionedThroughput.ReadCapacityUnits;
                        res.Table.ProvisionedThroughput.WriteCapacityUnits |]
            true
        with
        | :? Amazon.DynamoDBv2.Model.ResourceNotFoundException ->
            logDebug "Table [{0}] not found" [| tableName |]
            false

    [<AutoOpen>]
    module private SyntacticSugar = 
        let inline attribute x = new AttributeValue(S = x)
        let inline attribtues xs = 
            let xs = xs |> Array.map attribute
            new List<AttributeValue>(xs)
        let expected x  = new ExpectedAttributeValue(
                                Exists = true,
                                Value = attribute x)
        let notExpected = new ExpectedAttributeValue(Exists = false)
        let inline updatePut x = new AttributeValueUpdate(
                                        Action = AttributeAction.PUT, 
                                        Value  = attribute x)
        let updateDel = new AttributeValueUpdate(Action = AttributeAction.DELETE)

    /// Creates a table in DynamoDB with the specified config and name
    let createTable (dynamoDB : IAmazonDynamoDB) 
                    (config : ReactoKinesixConfig) 
                    (TableName tableName) =
        try
            let req = new CreateTableRequest(TableName = tableName)
            req.AttributeDefinitions.Add(
                new AttributeDefinition(
                    AttributeName = shardIdAttr, 
                    AttributeType = ScalarAttributeType.S))
            req.KeySchema.Add(
                new KeySchemaElement(
                    AttributeName = shardIdAttr, 
                    KeyType = KeyType.HASH))
            let throughput = 
                new ProvisionedThroughput(
                    ReadCapacityUnits  = config.DynamoDBReadThroughput,
                    WriteCapacityUnits = config.DynamoDBWriteThroughput)
            req.ProvisionedThroughput <- throughput
        
            let res = dynamoDB.CreateTable(req)
            logDebug "Created state table [{0}], current status [{1}], read throughput [{2}], write throughput [{3}]"
                      [| tableName
                         res.TableDescription.TableStatus
                         res.TableDescription.ProvisionedThroughput.ReadCapacityUnits
                         res.TableDescription.ProvisionedThroughput.WriteCapacityUnits |]

            Success ()
        with
        | :? Amazon.DynamoDBv2.Model.ResourceInUseException as exn -> 
            // already exists (perhaps race condition with multiple workers trying
            // to create table at the same time)
            logWarn exn 
                    "(Possible race condition) State table [{0}] already exists" 
                    [| tableName |]
            Success ()
        | exn -> Failure exn

    /// Initializes the application state table if necessary and returns the table name
    let initStateTable (dynamoDB : IAmazonDynamoDB) 
                       (config : ReactoKinesixConfig) tableName =
        try
            match doesTableExist dynamoDB tableName with
            | true -> Success ()
            | _    -> createTable dynamoDB config tableName
        with
        | exn -> Failure exn

    /// Waits till the DynamoDB table is ready
    let rec awaitStateTableReady (dynamoDB : IAmazonDynamoDB) 
                                 (TableName tableName as tn) =
        async {
            let req = new DescribeTableRequest(TableName = tableName)
            let! res = dynamoDB.DescribeTableAsync(req) |> Async.AwaitTask

            logDebug "State table [{0}] current status [{1}]" 
                     [| tableName; res.Table.TableStatus |]
            
            if res.Table.TableStatus = TableStatus.CREATING then
                do! Async.Sleep(1000)
                return! awaitStateTableReady dynamoDB tn
            else logDebug "State table [{0}] is considered ready (not in CREATING status)" 
                          [| tableName |]
        }

    /// Puts a shard into the shard conditionally against the worker ID so 
    /// that if another worker has already added the shard then we don't proceed
    let createShard (dynamoDB : IAmazonDynamoDB) 
                    (TableName tableName) 
                    (WorkerId workerId) 
                    (ShardId shardId) =
        async {
            logDebug "Creating shard [{2}] data in state table [{0}] for worker [{1}]" 
                     [| tableName; workerId; shardId |]

            let req = new PutItemRequest(TableName = tableName)
            req.Item.Add(shardIdAttr,       attribute shardId)
            req.Item.Add(workerIdAttr,      attribute workerId)
            req.Item.Add(lastHeartbeatAttr, attribute <| currentTimestamp())
        
            req.Expected.Add(workerIdAttr, notExpected)

            try
                let! _ = dynamoDB.PutItemAsync(req) |> Async.AwaitTask

                logDebug "Created shard [{2}] data in state table [{0}] for worker [{1}]" 
                         [| tableName; workerId; shardId |]
                return Success ()
            with
            | Flatten exn ->
                match exn with
                | :? ConditionalCheckFailedException ->
                    logDebug "(Conditional Check) Failed to create shard [{2}] data in state table [{0}] for worker [{1}]. Shard already exists in table."
                             [| tableName; workerId; shardId |]
                    return Success ()
                | exn ->
                    logError exn 
                             "Failed to create shard [{2}] data in state table [{0}] for worker [{1}]" 
                             [| tableName; workerId; shardId |]
                    return Failure exn
        }

    /// Returns the current status of the shard given the item retrieved from DynamoDB
    let private getShardStatusInternal shardId heartbeatTimeout = function
        | NoShard       -> ShardStatus.NotFound(shardId)
        | ClosedShard   -> ShardStatus.Closed(shardId)
        | Shard(workerId, heartbeat, checkpoint, handoverReq) ->
            let now = DateTime.UtcNow

            let seqNum = match checkpoint with 
                         | Some seqNum -> Some(SequenceNumber seqNum)
                         | _ -> None

            let isHeartbeatExpired = (now - heartbeat) > heartbeatTimeout
            let isHandoverExpired  = match handoverReq with
                                     | None -> true
                                     | Some { Expiry = expiry } -> now >= expiry

            match isHeartbeatExpired, isHandoverExpired with
            | true,  true  -> 
                ShardStatus.NotProcessing(shardId, workerId, heartbeat, seqNum)            
            | true,  false -> 
                let (Some { FromWorker = fromWorker; ToWorker = toWorker }) = handoverReq
                ShardStatus.HandingOver(shardId, WorkerId fromWorker, WorkerId toWorker, seqNum)
            | false, true  -> 
                ShardStatus.Processing(shardId, workerId, seqNum, None)
            | false, false -> 
                ShardStatus.Processing(shardId, workerId, seqNum, handoverReq)
            
    /// Returns the current status of the shard
    let getShardStatus (dynamoDB : IAmazonDynamoDB) 
                       (config   : ReactoKinesixConfig)
                       (TableName tableName)
                       ((ShardId   shardIdStr) as shardId) =
        async {
            let req = new GetItemRequest(TableName = tableName, ConsistentRead = true)
            req.Key.Add(shardIdAttr, attribute shardIdStr)
            req.AttributesToGet.AddRange(
                [| shardIdAttr 
                   workerIdAttr
                   checkpointAttr
                   lastHeartbeatAttr
                   isClosedAttr
                   handoverReqAttr |])

            logDebug "Getting current status of shard [{0}] from state table [{1}]" 
                     [| shardId; tableName |]

            let! res = Async.WithRetry(
                        dynamoDB.GetItemAsync(req) |> Async.AwaitTask, 
                        config.MaxDynamoDBRetries)

            return res 
                   |> Result.Bind (fun res -> 
                        getShardStatusInternal 
                            shardId 
                            config.HeartbeatTimeout 
                            res.Item)
        }

    /// Returns the current status fo all active (not-closed) shards
    let getShardStatuses (dynamoDB : IAmazonDynamoDB)
                         (config   : ReactoKinesixConfig)
                         (TableName tableName) =
        let rec scanTable acc exclusiveStartKey =
            async {
                let req = new ScanRequest(TableName = tableName)
                match exclusiveStartKey with
                | Some startKey -> req.ExclusiveStartKey <- startKey
                | _ -> ()

                req.AttributesToGet.AddRange(
                    [| shardIdAttr
                       workerIdAttr
                       checkpointAttr
                       lastHeartbeatAttr
                       isClosedAttr
                       handoverReqAttr |])
                let condition = 
                    new Condition(
                        ComparisonOperator = ComparisonOperator.NE,
                        AttributeValueList = attribtues [| "True" |])
                req.ScanFilter.Add(isClosedAttr, condition)

                let! res = Async.WithRetry(
                            dynamoDB.ScanAsync(req) |> Async.AwaitTask, 
                            config.MaxDynamoDBRetries)

                match res with
                // when LastEvaluatedKey is null/empty it means the last page 
                // of results have been process and there's no more data to 
                // be retrieved
                | Success res when res.LastEvaluatedKey = null || res.LastEvaluatedKey.Count = 0 ->
                    let shardsAsItems = (res.Items :: acc) |> Seq.collect id                     
                    let shardStatuses = 
                        shardsAsItems 
                        |> Seq.map (fun items ->
                            let shardId = ShardId items.[shardIdAttr].S
                            getShardStatusInternal shardId config.HeartbeatTimeout items)
                        |> Seq.toArray
                    return Success shardStatuses
                | Success res ->
                    return! scanTable (res.Items :: acc) (Some res.LastEvaluatedKey)
                | Failure exn -> 
                    return Failure exn
            }

        async { return! scanTable [] None }

    /// Updates a shard conditionally against the worker ID so that if for some
    /// reason another worker has taken over processing of this shard then we 
    /// shall stop further processing
    let private updateShard (update   : UpdateItemRequest -> unit) 
                            (dynamoDB : IAmazonDynamoDB) 
                            (TableName  tableName) 
                            (WorkerId   workerId) 
                            (ShardId    shardId) =
        async {
            let req = new UpdateItemRequest(TableName = tableName)
            req.Key.Add(shardIdAttr, attribute shardId)
            req.Expected.Add(workerIdAttr, expected workerId)

            // whilst updating the table, always update the heartbeat whilst we're at it
            req.AttributeUpdates.Add(
                lastHeartbeatAttr, 
                updatePut <| currentTimestamp())
        
            update req

            // exception handling for this is done in the client code based on 
            // the operation (heartbeat or checkpoint)
            let! _ = dynamoDB.UpdateItemAsync(req) |> Async.AwaitTask

            logDebug "Updated shard [{0}] for worker [{1}] in state table [{0}]" 
                     [| tableName; workerId; shardId |]
        }

    /// Updates the heartbeat value for the specified shard conditionally against
    /// the worker ID so that if for some reason another worker has taken over 
    /// this shard then we shall stop processing this shard
    let updateHeartbeat : IAmazonDynamoDB -> TableName -> WorkerId -> ShardId -> Async<unit> = 
        updateShard ignore

    /// Updates the sequence number checkpoint for the specified shard 
    /// conditionally against the worker ID so that if for some reason another
    /// worker has taken over this shard then we shall stop processing this shard
    let updateCheckpoint (timeBehind : TimeSpan) (SequenceNumber seqNumber) = 
        let update (req : UpdateItemRequest) = 
            req.AttributeUpdates.Add(
                checkpointAttr, 
                updatePut seqNumber)
            req.AttributeUpdates.Add(
                checkpointAtAttr, 
                updatePut <| currentTimestamp())
            req.AttributeUpdates.Add(
                timeBehindAttr,
                updatePut <| timeBehind.TotalMilliseconds.ToString())
                    
        updateShard update

    /// Updates the IsClosed flag for the specified shard conditionaly against 
    /// the worker ID so that if for some reason another worker has taken over 
    /// this shard then we shall stop processing this shard
    let updateIsClosed (isClosed : bool) =
        let update (req : UpdateItemRequest) =
            req.AttributeUpdates.Add(
                isClosedAttr, 
                updatePut <| isClosed.ToString())
                        
        updateShard update

    /// Updates the worker ID field for the specified shard conditionaly against 
    /// the old worker ID
    let updateWorkerId (WorkerId newWorkerId) =
        let update (req : UpdateItemRequest) =
            req.AttributeUpdates.Add(workerIdAttr, updatePut newWorkerId)
            req.AttributeUpdates.Add(handoverReqAttr, updateDel)

        updateShard update

    /// Issues a handover request
    let issueHandoverRequest (dynamoDB : IAmazonDynamoDB)
                             (config   : ReactoKinesixConfig)
                             (TableName  tableName) 
                             (WorkerId   fromWorkerId)
                             (WorkerId   toWorkerId) 
                             (ShardId    shardId) =
        async {
            let req = new UpdateItemRequest(TableName = tableName)
            req.Key.Add(shardIdAttr, attribute shardId)
            req.Expected.Add(handoverReqAttr, notExpected)

            let expiry = DateTime.UtcNow.Add(config.HandoverRequestExpiry)
            let handoverReq =
                {
                    FromWorker = fromWorkerId
                    ToWorker   = toWorkerId
                    Expiry     = expiry
                }
            req.AttributeUpdates.Add(
                handoverReqAttr, 
                updatePut <| serialize handoverReq)
            
            // exception handling for this is done in the client code based on the operation (heartbeat or checkpoint)
            let! _ = dynamoDB.UpdateItemAsync(req) |> Async.AwaitTask

            logDebug "Issued handover request for shard [{0}] from worker [{1}] to worker [{2}] in table [{3}], expiring at [{4}]" 
                     [| shardId; fromWorkerId; toWorkerId; tableName; expiry |]
        }
        |> Async.Catch

    /// Returns the pending handover request for this shard as an option
    let getHandoverRequest (dynamoDB : IAmazonDynamoDB)
                           (config   : ReactoKinesixConfig)
                           (TableName tableName)
                           (ShardId   shardId) =
        async {
            let req = new GetItemRequest(TableName = tableName, ConsistentRead = true)
            req.Key.Add(shardIdAttr, attribute shardId)
            req.AttributesToGet.Add(handoverReqAttr)

            logDebug "Getting pending handover request for shard [{0}] in state table [{1}]" 
                     [| shardId; tableName |]

            let! res = Async.WithRetry(
                        dynamoDB.GetItemAsync(req) |> Async.AwaitTask, 
                        config.MaxDynamoDBRetries)

            match res with
            | Failure exn -> return Failure exn
            | Success res -> 
                match tryGetAttributeValue res.Item handoverReqAttr with
                | Some str -> return Success (Some <| deserializeHandoverReq str)
                | _        -> return Success None
        }