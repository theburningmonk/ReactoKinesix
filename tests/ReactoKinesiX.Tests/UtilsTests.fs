namespace ReactoKinesix.Tests

open System
open System.Collections.Generic
open System.IO
open System.Reactive.Linq
open System.Threading
open System.Threading.Tasks

open Foq
open FsUnit
open NUnit.Framework

open Amazon.CloudWatch
open Amazon.CloudWatch.Model
open Amazon.DynamoDBv2
open Amazon.DynamoDBv2.Model
open Amazon.Kinesis
open Amazon.Kinesis.Model

open ReactoKinesix.Model
open ReactoKinesix.Utils

[<AutoOpen>]
module Helpers =     
    let isNullOrWs    = String.IsNullOrWhiteSpace
    let isNotNullOrWs = isNullOrWs >> not
    let isDefaultOrEmpty (xs : 'a seq) = xs = Unchecked.defaultof<'a seq> || Seq.length xs = 0

[<TestFixture>]
type UtilsTests () =
    [<Test>] 
    member test.``withRetry should retry a function up to N times`` () =
        let n = ref 0
        let f () = 
            n := !n + 1
            failwith "Boom"

        (fun () -> withRetry 3 f |> ignore) |> should throw typeof<System.Exception>
        !n |> should equal 4 // once + 3 retries = 4 times f should be run

    [<Test>]
    member test.``memoize should remember results of previously tried inputs to a function`` () =
        let n = ref 0
        let f x =
            n := !n + 1
            x + 1

        let memoizedF = memoize f
        { 1..10 } |> Seq.iter (fun _ -> memoizedF 42 |> ignore)
        !n |> should equal 1 // the memoized function f should have run only once

    [<Test>]
    member test.``Flatten lifts inner exception out of aggregate exceptions`` () =
        let f () = raise <| AggregateException(new InvalidOperationException())
        let liftExn f = 
            try 
                f() 
            with 
            | Flatten exn -> raise exn

        (fun () -> liftExn f |> ignore) |> should throw typeof<InvalidOperationException>

    [<Test>]
    member test.``Flatten lifts inner exception out of nested aggregate exceptions`` () =
        let f () = raise <| AggregateException(new InvalidOperationException())
        let g () = try f() with | exn -> raise <| AggregateException(exn)

        let liftExn f = 
            try 
                f() 
            with 
            | Flatten exn -> raise exn

        (fun () -> liftExn g |> ignore) |> should throw typeof<InvalidOperationException>

    [<Test>]
    member test.``Observable.FromAsync should complete when the aysnc workflow is finished`` () =
        let computation = async { return () }

        let n = ref 0
        Observable.FromAsync computation
        |> Observable.subscribe (fun _ -> n := !n + 1)
        
        !n |> should equal 1        

    [<Test>]
    member test.``Async.WithRetry should retry an async computation up to N times`` () =
        let n = ref 0
        let computation = async { 
            n := !n + 1
            failwith "Boom"
        }

        let res = Async.WithRetry (computation, 3) |> Async.RunSynchronously
        match res with | Failure _ -> true
        |> should equal true

        !n |> should equal 4 // initial attempt plus 3 retries should equal 4 runs in total

    [<Test>]
    member test.``Async.WithRetry should return success on first successful run`` () =
        let n = ref 0
        let computation = async { 
            n := !n + 1
            if !n < 4 then failwith "Boom"
            return !n
        }

        let res = Async.WithRetry (computation, 3) |> Async.RunSynchronously
        match res with | Success 4 -> true
        |> should equal true

        !n |> should equal 4 // initial attempt plus 3 retries should equal 4 runs in total

    [<Test>]
    member test.``MailboxProcessor.StartProtected should stop agent from being stopped by unhandled exceptions`` () =
        let n, restarts = ref 0, ref 0
        let agent = Agent.StartProtected<int>((fun inbox ->
            async {
                while true do
                    let! msg = inbox.Receive()
                    match msg with
                    | _ -> 
                        n := !n + 1
                        failwith "Boom"
            }), onRestart = fun _ -> restarts := !restarts + 1)

        { 1..10 } |> Seq.iter agent.Post

        Thread.Sleep(1000)
        !n        |> should equal 10
        !restarts |> should equal 10

    [<Test>]
    member test.``Seq.groupsOfAtMost should group items into groups of at most N elements`` () =
        let arr = [| 1..100 |]
        let equalSizedGroups = arr |> Seq.groupsOfAtMost 10 |> Seq.toArray
        equalSizedGroups.Length |> should equal 10
        equalSizedGroups |> Seq.iter (fun arr -> arr.Length |> should equal 10)

        let unequalSizedGroups = arr |> Seq.groupsOfAtMost 33 |> Seq.toArray
        unequalSizedGroups.Length |> should equal 4
        unequalSizedGroups.[0..2] |> Seq.iter (fun arr -> arr.Length |> should equal 33)
        unequalSizedGroups.[3].Length |> should equal 1

[<TestFixture>]
type CloudWatchUtilsTests () =
    [<Test>] 
    member test.``pushMetrics should push metrics in batches of 20`` () =
        let batches, metricCount = ref 0, ref 0
        let ns = "namespace"
        let cloudWatch =
            Mock<IAmazonCloudWatch>()
                .Setup(fun cw -> <@ cw.PutMetricDataAsync(any(), any()) @>)
                .Calls<PutMetricDataRequest * _>(fun (req, _) -> 
                    req.Namespace |> should equal ns

                    Interlocked.Increment(batches) |> ignore
                    Interlocked.Add(metricCount, req.MetricData.Count) |> ignore
                    Task.FromResult(new PutMetricDataResponse()))
                .Create()

        let getMetric n = 
            {
                Dimensions  = [||]
                MetricName  = sprintf "Metric-%d" n
                Timestamp   = DateTime.UtcNow
                Unit        = StandardUnit.Count
                Average     = 0.0
                Sum         = 0.0
                Max         = 0.0
                Min         = 0.0
                Count       = 0.0
            }
        Seq.init 45 getMetric |> Seq.toArray |> CloudWatchUtils.pushMetrics cloudWatch ns |> Async.RunSynchronously

        !batches     |> should equal 3
        !metricCount |> should equal 45

[<TestFixture>]
type KinesisUtilsTests () =    
    [<Test>]
    member test.``getShards should handle stream with small number of shards`` () =
        let streamDescription = new StreamDescription(StreamName   = "stream",
                                                      StreamStatus = StreamStatus.ACTIVE)
        { 1..10 } 
        |> Seq.map (fun n -> new Shard(ShardId = sprintf "shard-%d" n))
        |> Seq.iter streamDescription.Shards.Add
        
        let response = new DescribeStreamResponse()
        response.StreamDescription <- streamDescription

        let kinesis = 
            Mock<IAmazonKinesis>()
                .Setup(fun k -> <@ k.DescribeStreamAsync(is(fun req -> isNullOrWs req.ExclusiveStartShardId), any()) @>)
                .Returns(Task.FromResult(response))
                .Create()

        let res = KinesisUtils.getShards kinesis (StreamName "stream") |> Async.RunSynchronously
        
        match res with | Success _ -> true
        |> should equal true

        let (Success shards) = res
        shards |> should haveLength 10
    
    [<Test>]
    member test.``getShards should handle stream with large number of shards`` () =
        let getResponse hasMore startN count = 
            let streamDescription = new StreamDescription(StreamName    = "stream",
                                                          StreamStatus  = StreamStatus.ACTIVE,
                                                          HasMoreShards = hasMore)
            { startN..startN + count - 1 } 
            |> Seq.map (fun n -> new Shard(ShardId = sprintf "shard-%d" n))
            |> Seq.iter streamDescription.Shards.Add
        
            new DescribeStreamResponse(StreamDescription = streamDescription)

        let kinesis = 
            Mock<IAmazonKinesis>()
                .Setup(fun k -> <@ k.DescribeStreamAsync(is(fun req -> isNullOrWs req.ExclusiveStartShardId), any()) @>)
                .Returns(Task.FromResult(getResponse true 1 10))
                .Setup(fun k -> <@ k.DescribeStreamAsync(is(fun req -> isNotNullOrWs req.ExclusiveStartShardId && req.ExclusiveStartShardId <> "shard-10"), any()) @>)
                .Raises<Exception>()
                .Setup(fun k -> <@ k.DescribeStreamAsync(is(fun req -> isNotNullOrWs req.ExclusiveStartShardId), any()) @>)
                .Returns(Task.FromResult(getResponse false 11 5))
                .Create()

        let res = KinesisUtils.getShards kinesis (StreamName "stream") |> Async.RunSynchronously
        
        match res with | Success _ -> true
        |> should equal true

        let (Success shards) = res
        shards |> should haveLength 15

    [<Test>]
    member test.``getShardIterator should return the sequence number at the specified position`` () =        
        let getResponse seqNum = new GetShardIteratorResponse(ShardIterator = seqNum)

        let kinesis = 
            Mock<IAmazonKinesis>()
                .Setup(fun k -> <@ k.GetShardIteratorAsync(is(fun req -> req.ShardIteratorType = ShardIteratorType.TRIM_HORIZON), any()) @>)
                .Returns(Task.FromResult(getResponse "trim_horizon"))
                .Setup(fun k -> <@ k.GetShardIteratorAsync(is(fun req -> req.ShardIteratorType = ShardIteratorType.AT_SEQUENCE_NUMBER), any()) @>)
                .Returns(Task.FromResult(getResponse "at_seq_num"))
                .Setup(fun k -> <@ k.GetShardIteratorAsync(is(fun req -> req.ShardIteratorType = ShardIteratorType.AFTER_SEQUENCE_NUMBER), any()) @>)
                .Returns(Task.FromResult(getResponse "after_seq_num"))
                .Setup(fun k -> <@ k.GetShardIteratorAsync(is(fun req -> req.ShardIteratorType = ShardIteratorType.LATEST), any()) @>)
                .Returns(Task.FromResult(getResponse "latest"))
                .Create()

        KinesisUtils.getShardIterator kinesis (StreamName "stream") (ShardId "shard-1") TrimHorizon 
        |> Async.RunSynchronously
        |> should equal "trim_horizon"

        KinesisUtils.getShardIterator kinesis (StreamName "stream") (ShardId "shard-1") (AtSequenceNumber(SequenceNumber ""))
        |> Async.RunSynchronously
        |> should equal "at_seq_num"

        KinesisUtils.getShardIterator kinesis (StreamName "stream") (ShardId "shard-1") (AfterSequenceNumber(SequenceNumber ""))
        |> Async.RunSynchronously
        |> should equal "after_seq_num"

        KinesisUtils.getShardIterator kinesis (StreamName "stream") (ShardId "shard-1") Latest
        |> Async.RunSynchronously
        |> should equal "latest"

    [<Test>]
    member test.``getRecords should return failure when iterator is at end of shard`` () =
        let kinesis = Mock<IAmazonKinesis>().Create()
        let config  = new ReactoKinesixConfig()
        let res     = KinesisUtils.getRecords kinesis config (StreamName "stream") (ShardId "shard-1") EndOfShard 100
                      |> Async.RunSynchronously

        match res with | Failure(ShardCannotBeIteratedException) -> true
        |> should equal true

    [<Test>]
    member test.``getRecords should use specified token when supplied with an iterator token`` () =
        let config   = new ReactoKinesixConfig()
        let response = new GetRecordsResponse()
        response.NextShardIterator <- "nxt_iterator"
        response.Records.Add(new Amazon.Kinesis.Model.Record(SequenceNumber = "seq_num", 
                                                             Data           = new MemoryStream(),
                                                             PartitionKey   = "partition_key"))

        let kinesis = 
            Mock<IAmazonKinesis>()
                .Setup(fun k -> <@ k.GetShardIteratorAsync(any(), any()) @>)
                .Raises(new Exception("GetShardIterator should not be called"))
                .Setup(fun k -> <@ k.GetRecordsAsync(is(fun req -> req.ShardIterator <> "iterator"), any()) @>)
                .Raises(new Exception("Wrong iterator"))
                .Setup(fun k -> <@ k.GetRecordsAsync(is(fun req -> req.ShardIterator = "iterator"), any()) @>)
                .Returns(Task.FromResult(response))
                .Create()

        let res = KinesisUtils.getRecords kinesis config (StreamName "stream") (ShardId "shard-1") (IteratorToken "iterator") 100
                  |> Async.RunSynchronously

        match res with | Success _ -> true
        |> should equal true

        let (Success(nxtIterator, records)) = res
        nxtIterator |> should equal "nxt_iterator"
        records     |> should haveLength 1
        records.[0] |> should equal { SequenceNumber = "seq_num"; Data = [||]; PartitionKey = "partition_key" }

    [<Test>]
    member test.``getRecords should call get record iterator if no iterator token is provided`` () =
        let config   = new ReactoKinesixConfig()
        let getRecordsRes = new GetRecordsResponse()
        getRecordsRes.NextShardIterator <- "nxt_iterator"
        getRecordsRes.Records.Add(new Amazon.Kinesis.Model.Record(SequenceNumber = "seq_num", 
                                                                  Data           = new MemoryStream(),
                                                                  PartitionKey   = "partition_key"))

        let kinesis = 
            Mock<IAmazonKinesis>()
                .Setup(fun k -> <@ k.GetShardIteratorAsync(any(), any()) @>)
                .Returns(Task.FromResult(new GetShardIteratorResponse(ShardIterator = "iterator")))
                .Setup(fun k -> <@ k.GetRecordsAsync(is(fun req -> req.ShardIterator <> "iterator"), any()) @>)
                .Raises(new Exception("Wrong iterator"))
                .Setup(fun k -> <@ k.GetRecordsAsync(is(fun req -> req.ShardIterator = "iterator"), any()) @>)
                .Returns(Task.FromResult(getRecordsRes))
                .Create()

        let res = KinesisUtils.getRecords kinesis config (StreamName "stream") (ShardId "shard-1") (NoIteratorToken Latest) 100
                  |> Async.RunSynchronously

        match res with | Success _ -> true
        |> should equal true

        let (Success(nxtIterator, records)) = res
        nxtIterator |> should equal "nxt_iterator"
        records     |> should haveLength 1
        records.[0] |> should equal { SequenceNumber = "seq_num"; Data = [||]; PartitionKey = "partition_key" }

[<TestFixture>]
type DynamoDBUtilsTests () =
    let dateTimeFormat        = "yyyy-MM-dd HH:mm:ss.fffffff"
    let attributeStrValue str = new AttributeValue(S = str)

    [<Test>]
    member test.``initStateTable should return success without trying to create table if table already exists`` () =
        let config   = new ReactoKinesixConfig()
        let tableDescription = new TableDescription()
        tableDescription.TableStatus <- TableStatus.ACTIVE
        tableDescription.ProvisionedThroughput <- new ProvisionedThroughputDescription(ReadCapacityUnits = 10L, WriteCapacityUnits = 10L)

        let dynamoDb = 
            Mock<IAmazonDynamoDB>()
                .Setup(fun d -> <@ d.DescribeTable(is(fun (req : DescribeTableRequest) -> req.TableName = "table")) @>)
                .Returns(new DescribeTableResponse(Table = tableDescription))
                .Create()
        
        let res = DynamoDBUtils.initStateTable dynamoDb config (TableName "table")
        match res with | Success _ -> true
        |> should equal true

    [<Test>]
    member test.``initStateTable should try to create table if table does not exist`` () =
        let config = new ReactoKinesixConfig()
        let tableDescription = new TableDescription()
        tableDescription.TableStatus <- TableStatus.ACTIVE
        tableDescription.ProvisionedThroughput <- new ProvisionedThroughputDescription(ReadCapacityUnits = 10L, WriteCapacityUnits = 10L)

        let createTableCalled = ref false

        let dynamoDb = 
            Mock<IAmazonDynamoDB>()
                .Setup(fun d -> <@ d.DescribeTable(is(fun (req : DescribeTableRequest) -> req.TableName = "table")) @>)
                // because the ResourceNotFoundException has an internal constructor, hence this hack
                .Raises(TestUtils.UnsafeInit<Amazon.DynamoDBv2.Model.ResourceNotFoundException>())
                .Setup(fun d -> <@ d.CreateTable(is(fun req -> req.TableName = "table")) @>)
                .Calls<CreateTableResponse>(fun _ -> 
                    createTableCalled := true
                    new CreateTableResponse(TableDescription = tableDescription))
                .Create()
        
        let res = DynamoDBUtils.initStateTable dynamoDb config (TableName "table")
        match res with | Success _ -> true
        |> should equal true

        !createTableCalled |> should equal true

    [<Test>]
    member test.``createTable counts ResourceInUseException as success`` () =
        let config = new ReactoKinesixConfig()

        let dynamoDb = 
            Mock<IAmazonDynamoDB>()
                .Setup(fun d -> <@ d.CreateTable(is(fun req -> req.TableName = "table")) @>)
                .Raises(TestUtils.UnsafeInit<Amazon.DynamoDBv2.Model.ResourceInUseException>())
                .Create()
        
        let res = DynamoDBUtils.createTable dynamoDb config (TableName "table")
        match res with | Success _ -> true
        |> should equal true

    [<Test>]
    member test.``createShard should succeed when the shard doesn't exist`` () =
        let dynamoDb = 
            Mock<IAmazonDynamoDB>()
                .Setup(fun d -> <@ d.PutItemAsync(any(), any()) @>)
                .Returns(Task.FromResult(new PutItemResponse()))
                .Create()
        
        let res = DynamoDBUtils.createShard dynamoDb (TableName "table") (WorkerId "worker") (ShardId "shard-1") |> Async.RunSynchronously
        match res with | Success _ -> true
        |> should equal true

    [<Test>]
    member test.``createShard should succeed when the shard was created by another worker`` () =
        let dynamoDb = 
            Mock<IAmazonDynamoDB>()
                .Setup(fun d -> <@ d.PutItemAsync(any(), any()) @>)
                .Calls<Task<PutItemResponse>>(fun _ -> 
                    Task.Run(fun () -> 
                        raise <| TestUtils.UnsafeInit<Amazon.DynamoDBv2.Model.ConditionalCheckFailedException>()
                        
                        // this is just to satisfy the type requirement
                        new PutItemResponse()))
                .Create()
        
        let res = DynamoDBUtils.createShard dynamoDb (TableName "table") (WorkerId "worker") (ShardId "shard-1") |> Async.RunSynchronously
        match res with | Success _ -> true
        |> should equal true

    [<Test>]
    member test.``getShardStatus should return NotProcessing if the shard's heartbeat is passed heartbeat timeout`` () =
        let config   = new ReactoKinesixConfig()

        let response = new GetItemResponse()
        response.Item.Add("ShardId", attributeStrValue "shard-1")
        response.Item.Add("WorkerId", attributeStrValue "worker")
        response.Item.Add("SequenceNumberCheckpoint", attributeStrValue "checkpoint")
        response.Item.Add("LastHeartbeat", DateTime.MinValue.ToString(dateTimeFormat) |> attributeStrValue)

        let dynamoDb = 
            Mock<IAmazonDynamoDB>()
                .Setup(fun d -> <@ d.GetItemAsync(any(), any()) @>)
                .Returns(Task.FromResult response)
                .Create()
        
        let res = DynamoDBUtils.getShardStatus dynamoDb config (TableName "table") (ShardId "shard-1") |> Async.RunSynchronously
        match res with | Success (ShardStatus.NotProcessing _) -> true
        |> should equal true

        let (Success(ShardStatus.NotProcessing(shardId, worker, heartbeat, seqNum))) = res
        shardId |> should equal <| ShardId "shard-1"
        worker  |> should equal <| WorkerId "worker"
        heartbeat   |> should equal DateTime.MinValue
        seqNum      |> should equal <| Some (SequenceNumber "checkpoint")

    [<Test>]
    member test.``getShardStatus should return HandingOver if the shard has a non-expired handover request`` () =
        let config   = new ReactoKinesixConfig()

        let response = new GetItemResponse()
        response.Item.Add("ShardId", attributeStrValue "shard-1")
        response.Item.Add("WorkerId", attributeStrValue "worker")
        response.Item.Add("SequenceNumberCheckpoint", attributeStrValue "checkpoint")
        response.Item.Add("LastHeartbeat", DateTime.MinValue.ToString(dateTimeFormat) |> attributeStrValue)
        let handoverReq = { FromWorker = "worker"; ToWorker = "new-worker"; Expiry = DateTime.UtcNow.AddMinutes(10.0) }
        response.Item.Add("HandoverRequest", serialize handoverReq |> attributeStrValue)

        let dynamoDb = 
            Mock<IAmazonDynamoDB>()
                .Setup(fun d -> <@ d.GetItemAsync(any(), any()) @>)
                .Returns(Task.FromResult response)
                .Create()

        let res = DynamoDBUtils.getShardStatus dynamoDb config (TableName "table") (ShardId "shard-1") |> Async.RunSynchronously
        match res with | Success (ShardStatus.HandingOver _) -> true
        |> should equal true

        let (Success(ShardStatus.HandingOver(shardId, fromWorker, toWorker, seqNum))) = res
        shardId     |> should equal <| ShardId "shard-1"
        fromWorker  |> should equal <| WorkerId "worker"
        toWorker    |> should equal <| WorkerId "new-worker"
        seqNum      |> should equal <| Some (SequenceNumber "checkpoint")

    [<Test>]
    member test.``getShardStatus should return Processing if heartbeat has not expired`` () =
        let config   = new ReactoKinesixConfig()

        let response = new GetItemResponse()
        response.Item.Add("ShardId", attributeStrValue "shard-1")
        response.Item.Add("WorkerId", attributeStrValue "worker")
        response.Item.Add("SequenceNumberCheckpoint", attributeStrValue "checkpoint")
        response.Item.Add("LastHeartbeat", DateTime.UtcNow.ToString(dateTimeFormat) |> attributeStrValue)
        let handoverReq = { FromWorker = "worker"; ToWorker = "new-worker"; Expiry = DateTime.UtcNow.AddMinutes(10.0) }
        response.Item.Add("HandoverRequest", serialize handoverReq |> attributeStrValue)

        let dynamoDb = 
            Mock<IAmazonDynamoDB>()
                .Setup(fun d -> <@ d.GetItemAsync(any(), any()) @>)
                .Returns(Task.FromResult response)
                .Create()

        let res = DynamoDBUtils.getShardStatus dynamoDb config (TableName "table") (ShardId "shard-1") |> Async.RunSynchronously
        match res with | Success (ShardStatus.Processing _) -> true
        |> should equal true

        let (Success(ShardStatus.Processing(shardId, worker, seqNum, handoverReq'))) = res
        shardId      |> should equal <| ShardId "shard-1"
        worker       |> should equal <| WorkerId "worker"
        seqNum       |> should equal <| Some (SequenceNumber "checkpoint")
        handoverReq'.IsSome |> should equal true
        handoverReq'.Value.FromWorker |> should equal "worker"
        handoverReq'.Value.ToWorker   |> should equal "new-worker"

        // compare the expiry only upto the the second to negate any lose of precision from serialization & deserialization
        handoverReq'.Value.Expiry.ToString "yyyy-MM-dd HH:mm:ss"
        |> should equal <| handoverReq.Expiry.ToString "yyyy-MM-dd HH:mm:ss"
            
    [<Test>]
    member test.``getShardStatus should ignore expired handover requests`` () =
        let config   = new ReactoKinesixConfig()

        let response = new GetItemResponse()
        response.Item.Add("ShardId", attributeStrValue "shard-1")
        response.Item.Add("WorkerId", attributeStrValue "worker")
        response.Item.Add("SequenceNumberCheckpoint", attributeStrValue "checkpoint")
        response.Item.Add("LastHeartbeat", DateTime.UtcNow.ToString(dateTimeFormat) |> attributeStrValue)
        let handoverReq = { FromWorker = "worker"; ToWorker = "new-worker"; Expiry = DateTime.UtcNow.AddMinutes(-1.0) }
        response.Item.Add("HandoverRequest", serialize handoverReq |> attributeStrValue)

        let dynamoDb = 
            Mock<IAmazonDynamoDB>()
                .Setup(fun d -> <@ d.GetItemAsync(any(), any()) @>)
                .Returns(Task.FromResult response)
                .Create()

        let res = DynamoDBUtils.getShardStatus dynamoDb config (TableName "table") (ShardId "shard-1") |> Async.RunSynchronously
        match res with | Success (ShardStatus.Processing _) -> true
        |> should equal true

        let (Success(ShardStatus.Processing(shardId, worker, seqNum, handoverReq'))) = res
        shardId      |> should equal <| ShardId "shard-1"
        worker       |> should equal <| WorkerId "worker"
        seqNum       |> should equal <| Some (SequenceNumber "checkpoint")
        handoverReq'.IsNone |> should equal true
        
    [<Test>]
    member test.``getShardStatus should return Closed if the shard is marked as closed`` () =
        let config   = new ReactoKinesixConfig()

        let response = new GetItemResponse()
        response.Item.Add("ShardId", attributeStrValue "shard-1")
        response.Item.Add("WorkerId", attributeStrValue "worker")
        response.Item.Add("SequenceNumberCheckpoint", attributeStrValue "checkpoint")
        response.Item.Add("LastHeartbeat", DateTime.UtcNow.ToString(dateTimeFormat) |> attributeStrValue)
        response.Item.Add("IsClosed", attributeStrValue "true")

        let dynamoDb = 
            Mock<IAmazonDynamoDB>()
                .Setup(fun d -> <@ d.GetItemAsync(any(), any()) @>)
                .Returns(Task.FromResult response)
                .Create()

        let res = DynamoDBUtils.getShardStatus dynamoDb config (TableName "table") (ShardId "shard-1") |> Async.RunSynchronously
        match res with | Success (ShardStatus.Closed _) -> true
        |> should equal true

        let (Success(ShardStatus.Closed shardId)) = res
        shardId      |> should equal <| ShardId "shard-1"

    [<Test>]
    member test.``getShardStatus should return NotFound if the shard does not exist in the table`` () =
        let config   = new ReactoKinesixConfig()
        let response = new GetItemResponse()

        let dynamoDb = 
            Mock<IAmazonDynamoDB>()
                .Setup(fun d -> <@ d.GetItemAsync(any(), any()) @>)
                .Returns(Task.FromResult response)
                .Create()

        let res = DynamoDBUtils.getShardStatus dynamoDb config (TableName "table") (ShardId "shard-1") |> Async.RunSynchronously
        match res with | Success (ShardStatus.NotFound _) -> true
        |> should equal true

        let (Success(ShardStatus.NotFound shardId)) = res
        shardId      |> should equal <| ShardId "shard-1"

    [<Test>]
    member test.``getShardStatuses should try to filter out closed shards`` () =
        let config   = new ReactoKinesixConfig()

        let validReq (req : ScanRequest) =
            req.ScanFilter.ContainsKey "IsClosed" &&
            req.ScanFilter.["IsClosed"].ComparisonOperator = ComparisonOperator.NE &&
            req.ScanFilter.["IsClosed"].AttributeValueList.Count = 1 &&
            req.ScanFilter.["IsClosed"].AttributeValueList.[0].S = "True"

        let response = new ScanResponse()
        response.Count <- 1
        let item = new Dictionary<string, AttributeValue>()
        item.Add("ShardId", attributeStrValue "shard-1")
        item.Add("WorkerId", attributeStrValue "worker")
        item.Add("SequenceNumberCheckpoint", attributeStrValue "checkpoint")
        item.Add("LastHeartbeat", DateTime.MinValue.ToString(dateTimeFormat) |> attributeStrValue)
        response.Items.Add item

        let dynamoDb = 
            Mock<IAmazonDynamoDB>()
                .Setup(fun d -> <@ d.ScanAsync(is(validReq >> not), any()) @>)
                .Calls<ScanResponse>(fun _ -> 
                    Task.Run(fun () ->
                        failwith "IsClosed filter not found"

                        // this line is here just to satisfy type check
                        new ScanResponse()))
                .Setup(fun d -> <@ d.ScanAsync(is validReq, any()) @>)
                .Returns(Task.FromResult response)
                .Create()

        let res = DynamoDBUtils.getShardStatuses dynamoDb config (TableName "table") |> Async.RunSynchronously
        match res with | Success _ -> true
        |> should equal true

        let (Success statuses) = res
        statuses |> should haveLength 1
        match statuses.[0] with | ShardStatus.NotProcessing _ -> true
        |> should equal true

    [<Test>]
    member test.``getShardStatuses should handle stream with large number of shards`` () =
        let config   = new ReactoKinesixConfig()

        let getResponse lastEvalKey n =
            let response = new ScanResponse(Count = 1, LastEvaluatedKey = lastEvalKey)
            let item = new Dictionary<string, AttributeValue>()
            item.Add("ShardId", sprintf "shard-%d" n |> attributeStrValue)
            item.Add("WorkerId", attributeStrValue "worker")
            item.Add("SequenceNumberCheckpoint", attributeStrValue "checkpoint")
            item.Add("LastHeartbeat", DateTime.MinValue.ToString(dateTimeFormat) |> attributeStrValue)
            response.Items.Add item
            response

        let dynamoDb = 
            Mock<IAmazonDynamoDB>()
                .Setup(fun d -> <@ d.ScanAsync(is(fun req -> isDefaultOrEmpty req.ExclusiveStartKey), any()) @>)
                .Returns(fun () ->
                    let lastEvalKey = new Dictionary<string, AttributeValue>()
                    lastEvalKey.Add("ShardId", attributeStrValue "shard-1")
                    Task.FromResult(getResponse lastEvalKey 1))
                .Setup(fun d -> <@ d.ScanAsync(any(), any()) @>)
                .Returns(Task.FromResult(getResponse null 2))
                .Create()

        let res = DynamoDBUtils.getShardStatuses dynamoDb config (TableName "table") |> Async.RunSynchronously
        match res with | Success _ -> true
        |> should equal true

        let (Success statuses) = res
        statuses |> should haveLength 2

        // in whichever order, shard-1 and shard-2 should be present in the response
        { 1..2 } 
        |> Seq.forall (fun n -> 
            statuses |> Array.exists (fun status -> status.ShardId = ShardId(sprintf "shard-%d" n)))
        |> should equal true

    [<Test>]
    member test.``getHandoverRequest should return None when there is no handover request`` () =
        let config   = new ReactoKinesixConfig()
        let response = new GetItemResponse()

        let dynamoDb = 
            Mock<IAmazonDynamoDB>()
                .Setup(fun d -> <@ d.GetItemAsync(any(), any()) @>)
                .Returns(Task.FromResult response)
                .Create()

        let res = DynamoDBUtils.getHandoverRequest dynamoDb config (TableName "table") (ShardId "shard-1") |> Async.RunSynchronously
        match res with | Success None -> true
        |> should equal true

    [<Test>]
    member test.``getHandoverRequest should return Some when there is a handover request`` () =
        let config   = new ReactoKinesixConfig()
        let response = new GetItemResponse()
        response.Item.Add("ShardId", attributeStrValue "shard-1")
        response.Item.Add("WorkerId", attributeStrValue "worker")
        response.Item.Add("SequenceNumberCheckpoint", attributeStrValue "checkpoint")
        response.Item.Add("LastHeartbeat", DateTime.MinValue.ToString(dateTimeFormat) |> attributeStrValue)
        let handoverReq = { FromWorker = "worker"; ToWorker = "new-worker"; Expiry = DateTime.MaxValue }
        response.Item.Add("HandoverRequest", serialize handoverReq |> attributeStrValue)

        let dynamoDb = 
            Mock<IAmazonDynamoDB>()
                .Setup(fun d -> <@ d.GetItemAsync(any(), any()) @>)
                .Returns(Task.FromResult response)
                .Create()

        let res = DynamoDBUtils.getHandoverRequest dynamoDb config (TableName "table") (ShardId "shard-1") |> Async.RunSynchronously
        match res with | Success _ -> true
        |> should equal true

        let (Success(Some handoverReq)) = res
        handoverReq.FromWorker  |> should equal "worker"
        handoverReq.ToWorker    |> should equal "new-worker"
        handoverReq.Expiry.ToString("yyyy-MM-dd HH:mm:ss")      
        |> should equal <| DateTime.MaxValue.ToString("yyyy-MM-dd HH:mm:ss")