namespace ReactoKinesix.Utils

open System
open System.Collections.Generic
open System.Globalization
open System.Reactive.Linq

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

    /// Returns the shards that are part of the stream
    let getShards (kinesis : IAmazonKinesis) (StreamName streamName) =
        async {
            let req = new DescribeStreamRequest(StreamName = streamName)
            let! res = kinesis.DescribeStreamAsync(req)
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

            let res = kinesis.GetRecords(req)
            return res.NextShardIterator, res.Records :> Record seq
        }

module internal DynamoDBUtils =
    type IAmazonDynamoDB with
        member this.DescribeTableAsync req  = Async.FromBeginEnd(req, this.BeginDescribeTable, this.EndDescribeTable)
        member this.GetItemAsync req        = Async.FromBeginEnd(req, this.BeginGetItem, this.EndGetItem)        
        member this.ListTablesAsync req     = Async.FromBeginEnd(req, this.BeginListTables, this.EndListTables)
        member this.PutItemAsync req        = Async.FromBeginEnd(req, this.BeginPutItem, this.EndPutItem)
        member this.UpdateItemAsync req     = Async.FromBeginEnd(req, this.BeginUpdateItem, this.EndUpdateItem)

    let private shardIdAttr, lastHeartbeatAttr, workerIdAttr, checkpointAttr = 
        "ShardId", "LastHeartbeat", "WorkerId", "SequenceNumberCheckpoint"

    let private dateTimeFormat = "yyyy-MM-dd HH:mm:ss.fffffff"
    let private getHeartbeatTimestamp ()   = DateTime.UtcNow.ToString(dateTimeFormat)
    let private fromHeartbeatTimestamp str = DateTime.ParseExact(str, dateTimeFormat, CultureInfo.InvariantCulture)

    let private tryGetAttributeValue (dict : IDictionary<_, AttributeValue>) key = 
        match dict.TryGetValue key with
        | true, x -> Some x.S
        | _ -> None

    let private (|NoShard|Shard|) (res : GetItemResponse) =
        match tryGetAttributeValue res.Item shardIdAttr with
        | None -> NoShard
        | _ -> // the shard creation should always ensure that worker ID and heartbeat is created
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
            return res.TableNames
        }

    /// Initializes the application state table if necessary and returns the table name
    let initStateTable (dynamoDB : IAmazonDynamoDB) (config : ReactoKinesixConfig) appName =         
        let appTableName = sprintf "%s%s" appName config.DynamoDBTableSuffix

        async {
            let! tableNames = getTables dynamoDB

            match tableNames |> Seq.exists ((=) appTableName) with
            | false -> return TableName appTableName
            | _     -> 
                let req = new CreateTableRequest(TableName = appTableName)
                req.KeySchema.Add(new KeySchemaElement(AttributeName = shardIdAttr, KeyType = KeyType.HASH))
                req.ProvisionedThroughput.ReadCapacityUnits  <- config.DynamoDBReadThroughput
                req.ProvisionedThroughput.WriteCapacityUnits <- config.DynamoDBWriteThroughput
        
                // TODO : handle exception when table already exists
                let res = dynamoDB.CreateTable(req)

                return TableName res.TableDescription.TableName
        }

    /// Waits till the DynamoDB table is ready
    let rec awaitStateTableReady (dynamoDB : IAmazonDynamoDB) (TableName tableName as tn) =
        async {
            let req = new DescribeTableRequest(TableName = tableName)
            let! res = dynamoDB.DescribeTableAsync(req)

            do! Async.Sleep(1000)
            if res.Table.TableStatus = TableStatus.CREATING then
                return! awaitStateTableReady dynamoDB tn
        }

    /// Puts a shard into the shard conditionally against the worker ID so that if another worker has
    /// already added the shard then we don't proceed
    let createShard (dynamoDB : IAmazonDynamoDB) (TableName tableName) (WorkerId workerId) (ShardId shardId) =
        async {
            let req = new PutItemRequest(TableName = tableName)
            req.Item.Add(shardIdAttr, new AttributeValue(S = shardId))
            req.Item.Add(workerIdAttr, new AttributeValue(S = workerId))
            req.Item.Add(lastHeartbeatAttr, new AttributeValue(S = getHeartbeatTimestamp()))
        
            req.Expected.Add(workerIdAttr, new ExpectedAttributeValue(Exists = false))

            try
                do! dynamoDB.PutItemAsync(req) |> Async.Ignore
                return true
            with
            // TODO: handle case when conditional check failed (someone else already created the shard) differently
            // from other exceptions (throughput exceeded, etc.)
            | _ -> return false
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

            // TODO : handle exceptoins
            let! res = dynamoDB.GetItemAsync(req)

            match res with
            | NoShard -> return ShardStatus.Removed
            | Shard(workerId, heartbeat, checkpoint) ->
                let now = DateTime.UtcNow

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

            // TODO : handle exceptions - conditional check = stop, other = retry?
            do! dynamoDB.UpdateItemAsync(req) |> Async.Ignore
        }

    /// Updates the heartbeat value for the specified shard conditionally against the worker ID so that
    /// if for some reason another worker has taken over this shard then we shall stop processing this shard
    let updateHeartbeat : IAmazonDynamoDB -> TableName -> WorkerId -> ShardId -> Async<unit> = 
        let update (req : UpdateItemRequest) = req.Key.Add(lastHeartbeatAttr, new AttributeValue(S = getHeartbeatTimestamp()))

        updateShard update

    /// Updates the sequence number checkpoint for the specified shard conditionally against the worker
    /// ID so that if for some reason another worker has taken over this shard then we shall stop
    /// processing this shard
    let updateCheckpoint (SequenceNumber seqNumber) =
        let update (req : UpdateItemRequest) = 
            // whilst we're updating the checkpoint, might as well also update the heartbeat since it's
            // essentially a free update (i.e. one request)
            req.Key.Add(checkpointAttr,    new AttributeValue(S = seqNumber))
            req.Key.Add(lastHeartbeatAttr, new AttributeValue(S = getHeartbeatTimestamp()))
                        
        updateShard update