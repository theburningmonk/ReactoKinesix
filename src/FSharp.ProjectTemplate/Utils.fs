namespace ReactoKinesix.Utils

open System
open System.Collections.Generic
open System.Globalization
open System.Reactive.Linq

open log4net

open Amazon.DynamoDBv2
open Amazon.DynamoDBv2.Model
open Amazon.Kinesis
open Amazon.Kinesis.Model

open ReactoKinesix.Model

[<AutoOpen>]
module internal Utils =
    let validateConfig (config : ReactoKinesixConfig) =
        if config.HeartbeatTimeout < config.Heartbeat then
            raise <| InvalidHeartbeatConfiguration(config.Heartbeat, config.HeartbeatTimeout)

    let log (logger : ILog) format (args : obj[]) = 
        if logger.IsDebugEnabled then logger.DebugFormat(format, args)

    let logException (logger : ILog) exn format (args : obj[]) = 
        if logger.IsErrorEnabled then 
            let msg = String.Format(format, args)
            logger.Error(msg, exn)

    /// Extension methods to the Rx Observable type
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

module internal KinesisUtils =
    type IAmazonKinesis with
        member this.GetRecordsAsync req       = Async.FromBeginEnd(req, this.BeginGetRecords, this.EndGetRecords)
        member this.GetShardIteratorAsync req = Async.FromBeginEnd(req, this.BeginGetShardIterator, this.EndGetShardIterator)        
        member this.DescribeStreamAsync req   = Async.FromBeginEnd(req, this.BeginDescribeStream, this.EndDescribeStream)

    let private logger = LogManager.GetLogger("KinesisUtils")
    let private log    = log logger
    
    /// Returns the shards that are part of the stream
    let getShards (kinesis : IAmazonKinesis) (StreamName streamName) =
        async {
            let req = new DescribeStreamRequest(StreamName = streamName)
            let! res = kinesis.DescribeStreamAsync(req)         
               
            log "Stream [{0}] has [{1}] shards: [{2}]"
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

            log "Received shard iterator [{0}], type [{1}] for stream [{2}], shard [{3}]"
                [| res.ShardIterator; req.ShardIteratorType; streamName; shardId |]

            return res.ShardIterator
        }

    /// Returns a number of records from the shard in the stream along with the next shard iterator
    let getRecords (kinesis : IAmazonKinesis) streamName shardId iterator = 
        async {
            let req = GetRecordsRequest()

            match iterator with
            | IteratorToken(token) -> req.ShardIterator <- token
            | NoIteratorToken(iteratorType)   ->
                let! token = getShardIterator kinesis streamName shardId iteratorType
                req.ShardIterator <- token

            let! res = kinesis.GetRecordsAsync(req)

            log "Received [{0}] records from stream [{1}], shard [{2}]"
                [| res.Records.Count; streamName; shardId |]

            return res.NextShardIterator, res.Records :> Record seq
        }

module internal DynamoDBUtils =
    type IAmazonDynamoDB with
        member this.CreateTableAsync req    = Async.FromBeginEnd(req, this.BeginCreateTable, this.EndCreateTable)
        member this.DescribeTableAsync req  = Async.FromBeginEnd(req, this.BeginDescribeTable, this.EndDescribeTable)
        member this.GetItemAsync req        = Async.FromBeginEnd(req, this.BeginGetItem, this.EndGetItem)        
        member this.ListTablesAsync req     = Async.FromBeginEnd(req, this.BeginListTables, this.EndListTables)
        member this.PutItemAsync req        = Async.FromBeginEnd(req, this.BeginPutItem, this.EndPutItem)
        member this.UpdateItemAsync req     = Async.FromBeginEnd(req, this.BeginUpdateItem, this.EndUpdateItem)

    let private shardIdAttr, lastHeartbeatAttr, workerIdAttr, checkpointAttr = 
        "ShardId", "LastHeartbeat", "WorkerId", "SequenceNumberCheckpoint"

    let private logger       = LogManager.GetLogger("KinesisUtils")
    let private log          = log logger
    let private logException = logException logger

    let private dateTimeFormat = "yyyy-MM-dd HH:mm:ss.fffffff"
    let private getHeartbeatTimestamp ()   = DateTime.UtcNow.ToString(dateTimeFormat)
    let private fromHeartbeatTimestamp str = DateTime.ParseExact(str, dateTimeFormat, CultureInfo.InvariantCulture)

    let private tryGetAttributeValue (dict : IDictionary<_, AttributeValue>) key = 
        match dict.TryGetValue key with
        | true, x -> Some x.S
        | _ -> None

    let private (|NoShard|Shard|) (res : GetItemResponse) =
        if res.Item.Count = 0 then NoShard
        else 
            // the shard creation should always ensure that worker ID and heartbeat is created
            // but the checkpoint is only set the first time we were able to get records from
            // the stream and processed them
            let workerId, heartbeat, checkpoint = 
                res.Item.[workerIdAttr].S,
                res.Item.[lastHeartbeatAttr].S,
                tryGetAttributeValue res.Item checkpointAttr
            Shard(WorkerId workerId, fromHeartbeatTimestamp heartbeat, checkpoint)

    /// Returns the list of tables that currently exist in DynamoDB
    let getTables (dynamoDB : IAmazonDynamoDB) =
        async {
            let req  = new ListTablesRequest()
            let! res = dynamoDB.ListTablesAsync(req)

            log "DynamoDB returned [{0}] tables : [{1}]"
                [| res.TableNames.Count; String.Join(",", res.TableNames) |]

            return res.TableNames
        }

    /// Initializes the application state table if necessary and returns the table name
    let initStateTable (dynamoDB : IAmazonDynamoDB) (config : ReactoKinesixConfig) appName =
        let appTableName = sprintf "%s%s" appName config.DynamoDBTableSuffix

        log "Initiating state table [{0}] for app [{1}]" [| appTableName; appName |]

        async {
            let! tableNames = getTables dynamoDB

            match tableNames |> Seq.exists ((=) appTableName) with
            | true -> 
                log "State table [{0}] already exists for app [{1}]" [| appTableName; appName |]
                return TableName appTableName
            | _     -> 
                let req = new CreateTableRequest(TableName = appTableName)
                req.AttributeDefinitions.Add(new AttributeDefinition(AttributeName = shardIdAttr, AttributeType = ScalarAttributeType.S))
                req.KeySchema.Add(new KeySchemaElement(AttributeName = shardIdAttr, KeyType = KeyType.HASH))
                req.ProvisionedThroughput <- new ProvisionedThroughput(ReadCapacityUnits  = config.DynamoDBReadThroughput,
                                                                       WriteCapacityUnits = config.DynamoDBWriteThroughput)
        
                // TODO : handle exception when table already exists
                let! res = dynamoDB.CreateTableAsync(req)

                log "Created state table [{0}] for app [{1}], current status [{2}], read throughput [{3}], write throughput [{4}]"
                    [| appTableName; appName; res.TableDescription.TableStatus; 
                       res.TableDescription.ProvisionedThroughput.ReadCapacityUnits;
                       res.TableDescription.ProvisionedThroughput.WriteCapacityUnits |]

                return TableName res.TableDescription.TableName
        }

    /// Waits till the DynamoDB table is ready
    let rec awaitStateTableReady (dynamoDB : IAmazonDynamoDB) (TableName tableName as tn) =
        async {
            let req = new DescribeTableRequest(TableName = tableName)
            let! res = dynamoDB.DescribeTableAsync(req)

            log "State table [{0}] current status [{1}]" [| tableName; res.Table.TableStatus |]
            
            if res.Table.TableStatus = TableStatus.CREATING then
                do! Async.Sleep(1000)
                return! awaitStateTableReady dynamoDB tn
            else log "State table [{0}] is considered ready (not in CREATING status)" [| tableName |]
        }

    /// Puts a shard into the shard conditionally against the worker ID so that if another worker has
    /// already added the shard then we don't proceed
    let createShard (dynamoDB : IAmazonDynamoDB) (TableName tableName) (WorkerId workerId) (ShardId shardId) =
        async {
            log "Creating shard [{2}] data in state table [{0}] for worker [{1}]" [| tableName; workerId; shardId |]

            let req = new PutItemRequest(TableName = tableName)
            req.Item.Add(shardIdAttr, new AttributeValue(S = shardId))
            req.Item.Add(workerIdAttr, new AttributeValue(S = workerId))
            req.Item.Add(lastHeartbeatAttr, new AttributeValue(S = getHeartbeatTimestamp()))
        
            req.Expected.Add(workerIdAttr, new ExpectedAttributeValue(Exists = false))

            try
                do! dynamoDB.PutItemAsync(req) |> Async.Ignore

                log "Created shard [{2}] data in state table [{0}] for worker [{1}]" [| tableName; workerId; shardId |]
                return true
            with
            // TODO: handle case when conditional check failed (someone else already created the shard) differently
            // from other exceptions (throughput exceeded, etc.)
            | exn ->                 
                logException exn "Failed to create shard [{2}] data in state table [{0}] for worker [{1}]" [| tableName; workerId; shardId |]
                return false
        }

    /// Returns the current status of the shard
    let getShardStatus (dynamoDB : IAmazonDynamoDB) 
                       (config   : ReactoKinesixConfig)
                       (TableName tableName)
                       (ShardId   shardId) =
        async {
            let req = new GetItemRequest(TableName = tableName, ConsistentRead = true)
            req.Key.Add(shardIdAttr, new AttributeValue(S = shardId))
            req.AttributesToGet.AddRange([| workerIdAttr; checkpointAttr; lastHeartbeatAttr |])

            log "Getting current status of shard [{1}] from state table [{0}]" [| tableName; shardId |]

            // TODO : handle exceptions
            let! res = dynamoDB.GetItemAsync(req)

            match res with
            | NoShard -> 
                log "Shard [{1}] not found in state tabe [{0}]" [| tableName; shardId |]
                return ShardStatus.Removed
            | Shard(workerId, heartbeat, checkpoint) ->
                let now = DateTime.UtcNow

                log "Shard [{1}] found in state table [{0}], worker [{2}], last heartbeat [{3}], sequence number checkpoint [{4}]"
                    [| tableName; shardId; workerId; heartbeat; checkpoint |]

                match checkpoint with
                | None -> return ShardStatus.New(workerId, heartbeat)
                | Some seqNum when now - heartbeat < config.HeartbeatTimeout 
                    -> return ShardStatus.Processing(workerId, SequenceNumber seqNum)
                | Some seqNum -> return ShardStatus.NotProcessing(workerId, heartbeat, SequenceNumber seqNum)
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
        
            update req

            // exception handling for this is done in the client code based on the operation (heartbeat or checkpoint)
            do! dynamoDB.UpdateItemAsync(req) |> Async.Ignore

            log "Updated shard [{0}] for worker [{1}] in state table [{0}]" [| tableName; workerId; shardId |]
        }

    /// Updates the heartbeat value for the specified shard conditionally against the worker ID so that
    /// if for some reason another worker has taken over this shard then we shall stop processing this shard
    let updateHeartbeat : IAmazonDynamoDB -> TableName -> WorkerId -> ShardId -> Async<unit> = 
        let update (req : UpdateItemRequest) = 
            let newAttrValue = new AttributeValue(S = getHeartbeatTimestamp())
            req.AttributeUpdates.Add(lastHeartbeatAttr, new AttributeValueUpdate(Action = AttributeAction.PUT, Value = newAttrValue))

        updateShard update

    /// Updates the sequence number checkpoint for the specified shard conditionally against the worker
    /// ID so that if for some reason another worker has taken over this shard then we shall stop
    /// processing this shard
    let updateCheckpoint (SequenceNumber seqNumber) =
        let update (req : UpdateItemRequest) = 
            // whilst we're updating the checkpoint, might as well also update the heartbeat since it's
            // essentially a free update (i.e. one request)
            let newCheckpointValue = new AttributeValue(S = seqNumber)
            req.AttributeUpdates.Add(checkpointAttr, new AttributeValueUpdate(Action = AttributeAction.PUT, Value = newCheckpointValue))
            let newHeartbeatValue  = new AttributeValue(S = getHeartbeatTimestamp())
            req.AttributeUpdates.Add(lastHeartbeatAttr, new AttributeValueUpdate(Action = AttributeAction.PUT, Value = newHeartbeatValue))
                        
        updateShard update