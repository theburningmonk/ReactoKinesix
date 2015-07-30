namespace ReactoKinesix.Tests

open System.IO
open System.Threading

open FsUnit
open NUnit.Framework
open Foq
open log4net.Config

open Amazon.CloudWatch
open Amazon.CloudWatch.Model
open Amazon.DynamoDBv2
open Amazon.DynamoDBv2.Model
open Amazon.Kinesis
open Amazon.Kinesis.Model

open ReactoKinesix
open ReactoKinesix.Model
open ReactoKinesix.Utils

[<AutoOpen>]
module Tools = 
    do BasicConfigurator.Configure() |> ignore

    let doNothing = (fun _ -> ())

    let genFactory onNewProcessor onProcessed =
        { new IRecordProcessorFactory with
            member this.CreateNew _ = 
                onNewProcessor ()

                { new IRecordProcessor with
                    member this.Process(_shardId, records) = 
                        onProcessed records
                        { Status = Status.Success; Checkpoint = false }
                    member this.OnMaxRetryExceeded (_, _) = ()
                    member this.Dispose () = () } }

module ``Given that an application is starting`` = 
    [<Test>]
    let ``when it fails to initialize the state table it should except with InitializationFailedException`` () =
        let factory = genFactory doNothing doNothing

        let kinesis    = Mock<IAmazonKinesis>().Create()        
        let cloudWatch = Mock<IAmazonCloudWatch>().Create()
        let dynamoDb   = Mock<IAmazonDynamoDB>()
                            .Setup(fun d -> <@ d.DescribeTable(any<DescribeTableRequest>()) @>)
                            .Raises(TestUtils.UnsafeInit<Amazon.DynamoDBv2.Model.InternalServerErrorException>())
                            .Create()

        (fun () -> ReactoKinesixApp.CreateNew(kinesis, dynamoDb, cloudWatch, "app-1", "stream", "worker", factory) |> ignore)
        |> should throw typeof<InitializationFailedException>

    [<Test>]
    let ``when it finishes initializing it should create one processor for each shard`` () =
        let processors = ref 0
        let factory = genFactory (fun _ -> processors := !processors + 1) doNothing

        let cloudWatch = Mock<IAmazonCloudWatch>().Create()
        let kinesis    = new KinesisStub()
        (kinesis :> IAmazonKinesis).CreateStream(new CreateStreamRequest(StreamName = "stream", ShardCount = 1)) |> ignore
        kinesis.Streams.["stream"].Status <- StreamStatus.ACTIVE

        let dynamoDb   = new DynamoDBStub()

        let app = ReactoKinesixApp.CreateNew(kinesis, dynamoDb, cloudWatch, "app-2", "stream", "worker", factory)

        let table = dynamoDb.Tables.Values |> Seq.head
        table.Status <- TableStatus.ACTIVE

        // give it some time for the app to detect dynamodb table statuschange and then initialize the processors
        Thread.Sleep(3000)

        !processors |> should equal 1

        app.Dispose()

    [<Test>]
    let ``when it finishes initializing the processor should start processing available records`` () =
        let processed = ref 0
        let factory = genFactory doNothing (fun records -> processed := !processed + records.Length)

        let cloudWatch = Mock<IAmazonCloudWatch>().Create()
        let kinesis    = new KinesisStub()
        (kinesis :> IAmazonKinesis).CreateStream(new CreateStreamRequest(StreamName = "stream", ShardCount = 1)) |> ignore
        let stream     = kinesis.Streams.["stream"]
        stream.Status  <- StreamStatus.ACTIVE
        
        for i = 0 to 99 do
            let memStream = new MemoryStream([| byte i |])
            let req = new PutRecordRequest(StreamName = "stream", Data = memStream)
            stream.PutRecord(req) |> ignore

        let dynamoDb   = new DynamoDBStub()

        let app = ReactoKinesixApp.CreateNew(kinesis, dynamoDb, cloudWatch, "app-3", "stream", "worker", factory)

        let table = (dynamoDb.Tables |> Seq.head).Value
        table.Status <- TableStatus.ACTIVE

        Thread.Sleep(3000)

        !processed |> should equal 100

        app.Dispose()

    [<Test>]
    let ``when there is a shard being processed by another worker the application should not processing it`` () =
        let processed = ref 0
        let factory = genFactory doNothing (fun records -> processed := !processed + records.Length)

        let cloudWatch = Mock<IAmazonCloudWatch>().Create()
        let kinesis    = new KinesisStub()
        (kinesis :> IAmazonKinesis).CreateStream(new CreateStreamRequest(StreamName = "stream", ShardCount = 1)) |> ignore
        let stream     = kinesis.Streams.["stream"]
        stream.Status  <- StreamStatus.ACTIVE

        let dynamoDb   = new DynamoDBStub()
        let tableName  = "app-4KinesisState"

        DynamoDBUtils.createTable dynamoDb (new ReactoKinesixConfig()) (TableName tableName)                    |> ignore
        DynamoDBUtils.createShard dynamoDb (TableName tableName) (WorkerId "other-worker") (ShardId "shard-0")
        |> Async.RunSynchronously
        |> ignore
        dynamoDb.Tables.[tableName].Status <- TableStatus.ACTIVE

        for i = 0 to 99 do
            let memStream = new MemoryStream([| byte i |])
            let req = new PutRecordRequest(StreamName = "stream", Data = memStream)
            stream.PutRecord(req) |> ignore

        let app = ReactoKinesixApp.CreateNew(kinesis, dynamoDb, cloudWatch, "app-4", "stream", "worker", factory)

        Thread.Sleep(3000)

        !processed |> should equal 0

        app.Dispose()

module ``Given an error occurred`` =
    open System

    let recordEntry _ = 
        let entry = new PutRecordsRequestEntry()
        entry.Data <- new MemoryStream([| 42uy |])
        entry

    [<Test>]
    let ``when iterator is expired, should get new iterator and continue processing`` () =
        let processed = ref 0
        let factory =   
            genFactory 
                doNothing 
                (fun records ->
                    // make the first batch slow, and therefore 2nd batch need to 
                    // retry with seq number
                    if !processed = 0 then Thread.Sleep 150
                    processed := !processed + records.Length)

        let cloudWatch  = Mock<IAmazonCloudWatch>().Create()
        let kinesisStub = new KinesisStub(iteratorExpiry = TimeSpan.FromMilliseconds 100.0)
        let kinesis     = kinesisStub :> IAmazonKinesis
        kinesis.CreateStream(new CreateStreamRequest(StreamName = "stream", ShardCount = 1)) 
        |> ignore
        kinesisStub.Streams.["stream"].Status <- StreamStatus.ACTIVE

        let req = new PutRecordsRequest(StreamName = "stream")
        { 1..100 } |> Seq.map recordEntry |> req.Records.AddRange
        kinesis.PutRecords(req) |> ignore

        let dynamoDb = new DynamoDBStub()

        let config = ReactoKinesixConfig()
        config.MaxBatchSize <- 10
        let app = ReactoKinesixApp.CreateNew(
                    kinesis, dynamoDb, cloudWatch, "app-err", "stream", "worker", factory, config)

        let table = dynamoDb.Tables.Values |> Seq.head
        table.Status <- TableStatus.ACTIVE

        // give it some time for the app to detect dynamodb table statuschange and then initialize the processors
        Thread.Sleep(3000)
        
        !processed |> should equal 100

        app.Dispose()