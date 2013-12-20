namespace ReactoKinesix

open System
open System.Reactive
open System.Reactive.Linq

open Amazon
open Amazon.DynamoDBv2
open Amazon.DynamoDBv2.Model
open Amazon.Kinesis
open Amazon.Kinesis.Model

open ReactoKinesix.Model
open ReactoKinesix.Utils

type private ReactoKinesixProcessor (kinesis    : IAmazonKinesis,
                                     dynamoDB   : IAmazonDynamoDB,
                                     config     : ReactoKinesixConfig,
                                     tableName, 
                                     streamName,
                                     workerId,
                                     shardId,
                                     action     : Record -> unit) =
    let batchReceivedEvent          = new Event<Iterator * Record seq>()
    let recordProcessedEvent        = new Event<Record>()
    let processErroredEvent         = new Event<Record * Exception>()
    let batchProcessedEvent         = new Event<int * Iterator>()

    let heartbeatEvent              = new Event<unit>()
    let conditionalCheckFailedEvent = new Event<unit>()

    let catchCondCheck f = 
        try
            f()
        with
        | :? ConditionalCheckFailedException -> 
            conditionalCheckFailedEvent.Trigger()
            reraise()

    let updateHeartbeat _ = 
        (fun () -> DynamoDBUtils.updateHeartbeat dynamoDB tableName workerId shardId)
        |> catchCondCheck

    let updateCheckpoint seqNum =
        (fun () -> DynamoDBUtils.updateCheckpoint seqNum dynamoDB tableName workerId shardId)
        |> catchCondCheck

    let fetchNextRecords iterator = 
        async {
            let! nextIterator, batch = KinesisUtils.getRecords kinesis streamName shardId iterator
            batchReceivedEvent.Trigger(IteratorToken nextIterator, batch)
        }

    let processRecord record = 
        try 
            action(record)
            recordProcessedEvent.Trigger(record)
            Success(SequenceNumber record.SequenceNumber)
        with
        | ex -> 
            processErroredEvent.Trigger(record, ex)
            Failure(SequenceNumber record.SequenceNumber, ex)

    let stopProcessing  = conditionalCheckFailedEvent.Publish
    let heartbeatSub    = Observable.Interval(config.Heartbeat).TakeUntil(stopProcessing).Subscribe(updateHeartbeat)
    
    // until we need to stop processing, keep fetching new records when we've finished processing the previous batch
    let fetchSub        = batchProcessedEvent.Publish
                            .TakeUntil(stopProcessing)
                            .Subscribe(fun (_, iterator) -> fetchNextRecords iterator |> Async.StartImmediate)

    let receivedSub     = 
        batchReceivedEvent.Publish
            .TakeUntil(stopProcessing)
            .Subscribe(fun (iterator, records) ->
                match Seq.isEmpty records with
                | true -> batchProcessedEvent.Trigger(0, iterator)
                | _ -> 
                    let count, lastResult = 
                        records 
                        |> Seq.scan (fun (count, _) record -> 
                            let res = processRecord record
                            (count + 1, Some res)) (0, None)
                        |> Seq.takeWhile (fun (_, res) -> match res with | Some (Failure _) -> false | _ -> true)
                        |> Seq.reduce (fun _ lastRes -> lastRes)

                    match lastResult with
                    | Some (Success _)           -> batchProcessedEvent.Trigger(count, iterator)
                    | Some (Failure (seqNum, _)) -> batchProcessedEvent.Trigger(count, NoIteratorToken <| AtSequenceNumber seqNum))

    let initSub = Observable
                    .FromAsync(DynamoDBUtils.getShardStatus dynamoDB config tableName shardId)
                    .Subscribe(function 
                        | New 
                            -> // the shard has not been processed before, so start from the oldest record
                               fetchNextRecords (NoIteratorToken <| TrimHorizon) |> Async.StartImmediate
                        | NotProcessing(_, _, seqNum)
                            -> // the shard has not been processed currently, start from the last checkpoint
                               fetchNextRecords (NoIteratorToken <| AfterSequenceNumber seqNum) |> Async.StartImmediate
                        | Processing(workerId', seqNum) when workerId' = workerId 
                            -> // the shard was being processed by this worker, continue from where we left off
                               fetchNextRecords (NoIteratorToken <| AfterSequenceNumber seqNum) |> Async.StartImmediate
                        | _ -> ())

    [<CLIEvent>] member this.OnBatchReceived            = batchReceivedEvent.Publish
    [<CLIEvent>] member this.OnRecordProcessed          = recordProcessedEvent.Publish
    [<CLIEvent>] member this.OnProcessErrored           = processErroredEvent.Publish
    [<CLIEvent>] member this.OnBatchProcessed           = batchProcessedEvent.Publish
    [<CLIEvent>] member this.OnHeartbeat                = heartbeatEvent.Publish
    [<CLIEvent>] member this.OnConditionalCheckFailed   = conditionalCheckFailedEvent.Publish

    interface IDisposable with
        member this.Dispose () =
            heartbeatSub.Dispose()
            fetchSub.Dispose()
            receivedSub.Dispose()
            initSub.Dispose()

type private ReactoKinesix (streamName, workerId, shardId, config) =
    

    do ()

type ReactoKinesixApp (awsKey     : string, 
                       awsSecret  : string, 
                       region     : RegionEndpoint,
                       appName    : string,
                       streamName : string,
                       ?config    : ReactoKinesixConfig) =
    let config     = defaultArg config <| new ReactoKinesixConfig()
    do Utils.validateConfig config

    let kinesis    = AWSClientFactory.CreateAmazonKinesisClient(awsKey, awsSecret, region)
    let dynamoDB   = AWSClientFactory.CreateAmazonDynamoDBClient(awsKey, awsSecret, region) 
    
    let streamName = StreamName streamName

    // initialize the application
    let tableName  = DynamoDBUtils.initStateTable dynamoDB config appName
    let shards     = KinesisUtils.getShards kinesis streamName
    do shards 
       |> Seq.map (fun shard -> DynamoDBUtils.createShard dynamoDB tableName (WorkerId "ME") (ShardId shard.ShardId))
       |> Seq.iter (fun shard -> ())
