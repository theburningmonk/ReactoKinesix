namespace ReactoKinesix.Utils

open System
open System.Collections.Generic
open System.Globalization
open System.Reactive.Linq
open System.Threading
open System.Threading.Tasks

open log4net

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
            raise <| InvalidHeartbeatConfiguration(config.Heartbeat, config.HeartbeatTimeout)

        if config.MaxDynamoDBRetries < 0 then
            raise <| NegativeMaxDynamoDBRetriesConfiguration(config.MaxDynamoDBRetries)

        if config.MaxKinesisRetries < 0 then
            raise <| NegativeMaxKinesisRetriesConfiguration(config.MaxKinesisRetries)

    let logDebug (logger : ILog) format (args : obj[]) = if logger.IsDebugEnabled then logger.DebugFormat(format, args)
    let logInfo  (logger : ILog) format (args : obj[]) = if logger.IsInfoEnabled  then logger.InfoFormat(format, args)
    let logWarn  (logger : ILog) format (args : obj[]) = if logger.IsWarnEnabled  then logger.WarnFormat(format, args)    
    let logError (logger : ILog) exn format (args : obj[]) = 
        if logger.IsErrorEnabled then
            let msg = String.Format(format, args)
            logger.Error(msg, exn)

    /// Default function for calcuating delay (in milliseconds) between retries, based on (http://en.wikipedia.org/wiki/Exponential_backoff)
    /// TODO : can be memoized
    let private exponentialDelay attempts =
        let rec sum acc = function | 0 -> acc | n -> sum (acc + n) (n - 1)

        let n = pown 2 attempts - 1
        let slots = float (sum 0 n) / float (n + 1)
        int (500.0 * slots)

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
                    | Choice2Of2 _ when retryCount <= maxRetries -> 
                        do! calcDelay (retryCount + 1) |> Async.Sleep
                        return! loop (retryCount + 1)
                    | Choice2Of2 exn -> return Failure exn
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
            let watchdog f x = async {
                while true do
                    try
                        do! f x
                    with exn -> onRestart(exn)
            }

            Agent.Start((fun inbox -> watchdog body inbox), ?cancellationToken = cancellationToken)

module internal KinesisUtils =
    type IAmazonKinesis with
        member this.GetRecordsAsync req       = Async.FromBeginEnd(req, this.BeginGetRecords, this.EndGetRecords)
        member this.GetShardIteratorAsync req = Async.FromBeginEnd(req, this.BeginGetShardIterator, this.EndGetShardIterator)        
        member this.DescribeStreamAsync req   = Async.FromBeginEnd(req, this.BeginDescribeStream, this.EndDescribeStream)

    let private logger   = LogManager.GetLogger("KinesisUtils")
    let private logDebug = logDebug logger
    let private logError = logError logger
    
    /// Returns the shards that are part of the stream
    let getShards (kinesis : IAmazonKinesis) (StreamName streamName) =
        async {
            let req = new DescribeStreamRequest(StreamName = streamName)
            let! res = kinesis.DescribeStreamAsync(req)         
               
            logDebug "Stream [{0}] has [{1}] shards: [{2}]"
                     [| res.StreamDescription.StreamName
                        res.StreamDescription.Shards.Count
                        String.Join(",", res.StreamDescription.Shards |> Seq.map(fun shard -> shard.ShardId)) |]

            return res.StreamDescription.Shards
        }

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

            let! res = kinesis.GetShardIteratorAsync(req)

            logDebug "Received shard iterator [{0}], type [{1}] for stream [{2}], shard [{3}]"
                     [| res.ShardIterator; req.ShardIteratorType; streamName; shardId |]

            return res.ShardIterator
        }

    /// Returns a number of records from the shard in the stream along with the next shard iterator
    let getRecords (kinesis : IAmazonKinesis) 
                   (config : ReactoKinesixConfig) 
                   streamName 
                   shardId
                   iterator = 
        async {
            match iterator with
            | EndOfShard -> return Failure(ShardCannotBeIterated)
            | _ ->
                let req = GetRecordsRequest()

                match iterator with  
                | IteratorToken(token) -> req.ShardIterator <- token
                | NoIteratorToken(iteratorType) ->
                    let! token = getShardIterator kinesis streamName shardId iteratorType
                    req.ShardIterator <- token

                let! getRecordResult = Async.WithRetry(kinesis.GetRecordsAsync(req), config.MaxKinesisRetries)
                match getRecordResult with
                | Success res -> 
                    logDebug "Received [{0}] records from stream [{1}], shard [{2}]"
                             [| res.Records.Count; streamName; shardId |]

                    return Success(res.NextShardIterator, res.Records :> Record seq)
                | Failure exn ->
                    logError exn "Failed to get records from stream [{0}], shard [{1}]" [| streamName; shardId |]
                    return Failure(exn) 
        }

module internal DynamoDBUtils =
    type IAmazonDynamoDB with
        member this.CreateTableAsync req    = Async.FromBeginEnd(req, this.BeginCreateTable, this.EndCreateTable)
        member this.DescribeTableAsync req  = Async.FromBeginEnd(req, this.BeginDescribeTable, this.EndDescribeTable)
        member this.GetItemAsync req        = Async.FromBeginEnd(req, this.BeginGetItem, this.EndGetItem)        
        member this.ListTablesAsync req     = Async.FromBeginEnd(req, this.BeginListTables, this.EndListTables)
        member this.PutItemAsync req        = Async.FromBeginEnd(req, this.BeginPutItem, this.EndPutItem)
        member this.UpdateItemAsync req     = Async.FromBeginEnd(req, this.BeginUpdateItem, this.EndUpdateItem)

    let private shardIdAttr, lastHeartbeatAttr, workerIdAttr, checkpointAttr, isClosedAttr = 
        "ShardId", "LastHeartbeat", "WorkerId", "SequenceNumberCheckpoint", "IsClosed"

    let private logger   = LogManager.GetLogger("KinesisUtils")
    let private logDebug = logDebug logger
    let private logWarn  = logWarn  logger
    let private logError = logError logger

    let private dateTimeFormat = "yyyy-MM-dd HH:mm:ss.fffffff"
    let private getHeartbeatTimestamp ()   = DateTime.UtcNow.ToString(dateTimeFormat)
    let private fromHeartbeatTimestamp str = DateTime.ParseExact(str, dateTimeFormat, CultureInfo.InvariantCulture)

    let private tryGetAttributeValue (dict : IDictionary<_, AttributeValue>) key = 
        match dict.TryGetValue key with | true, x -> Some x.S | _ -> None
    let private parseBool (str : string) = match bool.TryParse(str) with | true, x -> x | _ -> false

    let private (|NoShard|ClosedShard|Shard|) (res : GetItemResponse) =
        if res.Item.Count = 0 then NoShard
        else 
            // the shard creation should always ensure that worker ID and heartbeat is created
            // but the checkpoint is only set the first time we were able to get records from
            // the stream and processed them
            let workerId, heartbeat, checkpoint, isClosed = 
                res.Item.[workerIdAttr].S,
                res.Item.[lastHeartbeatAttr].S,
                tryGetAttributeValue res.Item checkpointAttr,
                tryGetAttributeValue res.Item isClosedAttr

            match isClosed with
            | Some boolStr when parseBool boolStr -> ClosedShard
            | _ -> Shard(WorkerId workerId, fromHeartbeatTimestamp heartbeat, checkpoint)

    /// Returns the list of tables that currently exist in DynamoDB
    let getTables (dynamoDB : IAmazonDynamoDB) =
        async {
            let req  = new ListTablesRequest()
            let! res = dynamoDB.ListTablesAsync(req)

            logDebug "DynamoDB returned [{0}] tables : [{1}]"
                     [| res.TableNames.Count; String.Join(",", res.TableNames) |]

            return res.TableNames
        }

    /// Initializes the application state table if necessary and returns the table name
    let initStateTable (dynamoDB : IAmazonDynamoDB) (config : ReactoKinesixConfig) appName =
        let appTableName = sprintf "%s%s" appName config.DynamoDBTableSuffix

        logDebug "Initiating state table [{0}] for app [{1}]" [| appTableName; appName |]

        async {
            let! tableNames = getTables dynamoDB

            match tableNames |> Seq.exists ((=) appTableName) with
            | true -> 
                logDebug "State table [{0}] already exists for app [{1}]" [| appTableName; appName |]
                return Success(TableName appTableName)
            | _     -> 
                let req = new CreateTableRequest(TableName = appTableName)
                req.AttributeDefinitions.Add(new AttributeDefinition(AttributeName = shardIdAttr, AttributeType = ScalarAttributeType.S))
                req.KeySchema.Add(new KeySchemaElement(AttributeName = shardIdAttr, KeyType = KeyType.HASH))
                req.ProvisionedThroughput <- new ProvisionedThroughput(ReadCapacityUnits  = config.DynamoDBReadThroughput,
                                                                       WriteCapacityUnits = config.DynamoDBWriteThroughput)
        
                let! res = dynamoDB.CreateTableAsync(req) |> Async.Catch
                match res with
                | Choice1Of2 res -> 
                    logDebug "Created state table [{0}] for app [{1}], current status [{2}], read throughput [{3}], write throughput [{4}]"
                             [| appTableName; appName; res.TableDescription.TableStatus; 
                                res.TableDescription.ProvisionedThroughput.ReadCapacityUnits;
                                res.TableDescription.ProvisionedThroughput.WriteCapacityUnits |]

                    return Success(TableName appTableName)
                | Choice2Of2 exn ->
                    match exn with
                    | :? ResourceInUseException -> 
                        // already exists (perhaps race condition with multiple workers trying to create table at the same time)
                        logWarn "(Possible race condition) State table [{0}] already exists for app [{1}]" [| appTableName; appName |]
                        return Success(TableName appTableName)
                    | _ -> return Failure((), exn)
        }

    /// Waits till the DynamoDB table is ready
    let rec awaitStateTableReady (dynamoDB : IAmazonDynamoDB) (TableName tableName as tn) =
        async {
            let req = new DescribeTableRequest(TableName = tableName)
            let! res = dynamoDB.DescribeTableAsync(req)

            logDebug "State table [{0}] current status [{1}]" [| tableName; res.Table.TableStatus |]
            
            if res.Table.TableStatus = TableStatus.CREATING then
                do! Async.Sleep(1000)
                return! awaitStateTableReady dynamoDB tn
            else logDebug "State table [{0}] is considered ready (not in CREATING status)" [| tableName |]
        }

    /// Puts a shard into the shard conditionally against the worker ID so that if another worker has
    /// already added the shard then we don't proceed
    let createShard (dynamoDB : IAmazonDynamoDB) (TableName tableName) (WorkerId workerId) (ShardId shardId) =
        async {
            logDebug "Creating shard [{2}] data in state table [{0}] for worker [{1}]" [| tableName; workerId; shardId |]

            let req = new PutItemRequest(TableName = tableName)
            req.Item.Add(shardIdAttr, new AttributeValue(S = shardId))
            req.Item.Add(workerIdAttr, new AttributeValue(S = workerId))
            req.Item.Add(lastHeartbeatAttr, new AttributeValue(S = getHeartbeatTimestamp()))
        
            req.Expected.Add(workerIdAttr, new ExpectedAttributeValue(Exists = false))

            try
                do! dynamoDB.PutItemAsync(req) |> Async.Ignore

                logDebug "Created shard [{2}] data in state table [{0}] for worker [{1}]" [| tableName; workerId; shardId |]
                return Success ()
            with
            | :? ConditionalCheckFailedException ->
                logDebug "(Conditional Check) Failed to create shard [{2}] data in state table [{0}] for worker [{1}]. Shard already exists in table."
                         [| tableName; workerId; shardId |]
                return Success ()
            | exn ->
                logError exn "Failed to create shard [{2}] data in state table [{0}] for worker [{1}]" [| tableName; workerId; shardId |]
                return Failure exn
        }

    /// Returns the current status of the shard
    let getShardStatus (dynamoDB : IAmazonDynamoDB) 
                       (config   : ReactoKinesixConfig)
                       (TableName tableName)
                       (ShardId   shardId) =
        async {
            let req = new GetItemRequest(TableName = tableName, ConsistentRead = true)
            req.Key.Add(shardIdAttr, new AttributeValue(S = shardId))
            req.AttributesToGet.AddRange([| workerIdAttr; checkpointAttr; lastHeartbeatAttr; isClosedAttr |])

            logDebug "Getting current status of shard [{1}] from state table [{0}]" [| tableName; shardId |]

            let! res = Async.WithRetry(dynamoDB.GetItemAsync(req), config.MaxDynamoDBRetries)

            match res with
            | Failure exn -> return Failure exn
            | Success res -> 
                match res with
                | NoShard -> 
                    logDebug "Shard [{1}] not found in state tabe [{0}]" [| tableName; shardId |]
                    return Success ShardStatus.NotFound
                | ClosedShard -> 
                    logDebug "Shard [{0}] is closed" [| shardId |]
                    return Success ShardStatus.Closed
                | Shard(workerId, heartbeat, checkpoint) ->
                    let now = DateTime.UtcNow

                    logDebug "Shard [{1}] found in state table [{0}], worker [{2}], last heartbeat [{3}], sequence number checkpoint [{4}]"
                             [| tableName; shardId; workerId; heartbeat; checkpoint |]

                    match checkpoint with
                    | None -> return Success <| ShardStatus.New(workerId, heartbeat)
                    | Some seqNum when now - heartbeat < config.HeartbeatTimeout 
                        -> return Success <| ShardStatus.Processing(workerId, SequenceNumber seqNum)
                    | Some seqNum -> return Success <| ShardStatus.NotProcessing(workerId, heartbeat, SequenceNumber seqNum)
        }

    /// Updates a shard conditionally against the worker ID so that if for some reason another worker has
    /// taken over processing of this shard then we shall stop further processing
    let private updateShard (update   : UpdateItemRequest -> unit) 
                            (dynamoDB : IAmazonDynamoDB) 
                            (TableName  tableName) 
                            (WorkerId   workerId) 
                            (ShardId    shardId) =
        async {
            let req = new UpdateItemRequest(TableName = tableName)
            req.Key.Add(shardIdAttr, new AttributeValue(S = shardId))
        
            let expectedAttrVal = new ExpectedAttributeValue(Value = new AttributeValue(S = workerId), Exists = true)
            req.Expected.Add(workerIdAttr, expectedAttrVal)

            // whilst updating the table, always update the heartbeat whilst we're at it
            let newHeartbeatValue  = new AttributeValue(S = getHeartbeatTimestamp())
            req.AttributeUpdates.Add(lastHeartbeatAttr, new AttributeValueUpdate(Action = AttributeAction.PUT, Value = newHeartbeatValue))                
        
            update req

            // exception handling for this is done in the client code based on the operation (heartbeat or checkpoint)
            do! dynamoDB.UpdateItemAsync(req) |> Async.Ignore

            logDebug "Updated shard [{0}] for worker [{1}] in state table [{0}]" [| tableName; workerId; shardId |]
        }

    /// Updates the heartbeat value for the specified shard conditionally against the worker ID so that
    /// if for some reason another worker has taken over this shard then we shall stop processing this shard
    let updateHeartbeat : IAmazonDynamoDB -> TableName -> WorkerId -> ShardId -> Async<unit> = 
        updateShard (fun _ -> ())

    /// Updates the sequence number checkpoint for the specified shard conditionally against the worker
    /// ID so that if for some reason another worker has taken over this shard then we shall stop
    /// processing this shard
    let updateCheckpoint (SequenceNumber seqNumber) = 
        let update (req : UpdateItemRequest) = 
            let newCheckpointValue = new AttributeValue(S = seqNumber)
            req.AttributeUpdates.Add(checkpointAttr, new AttributeValueUpdate(Action = AttributeAction.PUT, Value = newCheckpointValue))
                    
        updateShard update

    /// Updates the IsClosed flag for the specified shard conditionaly against the worker ID so that if 
    /// for some reason another worker has taken over this shard then we shall stop processing this shard
    let updateIsClosed (isClosed : bool) =
        let update (req : UpdateItemRequest) = 
            let newIsClosedValue = new AttributeValue(S = isClosed.ToString())
            req.AttributeUpdates.Add(isClosedAttr, new AttributeValueUpdate(Action = AttributeAction.PUT, Value = newIsClosedValue))
                        
        updateShard update