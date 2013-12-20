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
    let batchProcessedEvent         = new Event<Iterator * SequenceNumber>()

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

    let fetchNextRecords seqNum iteratorType iterator = 
        async {
            let! nextIterator, batch = KinesisUtils.getRecords kinesis streamName shardId iteratorType iterator
            batchReceivedEvent.Trigger(Iterator(Some nextIterator), batch)
        }

    let processRecord (record : Record) =
        try 
            action(record)
            recordProcessedEvent.Trigger(record)
        with
        | ex -> processErroredEvent.Trigger(record, ex)

    let stopProcessing  = conditionalCheckFailedEvent.Publish
    let heartbeatSub    = Observable.Interval(config.Heartbeat).TakeUntil(stopProcessing).Subscribe(updateHeartbeat)
    
    // until we need to stop processing, keep fetching new records when we've finished processing the previous batch
    let fetchSub        = batchProcessedEvent.Publish
                            .TakeUntil(stopProcessing)
                            .Subscribe(fun (iterator, seqNum) -> Async.StartImmediate <| fetchNextRecords seqNum ShardIteratorType.AFTER_SEQUENCE_NUMBER iterator)

    let records         = batchReceivedEvent.Publish.SelectMany(fun (_, records) -> records)
    let recordsSub      = records.Subscribe(processRecord)

    let initSub = Observable
                    .FromAsync(DynamoDBUtils.getShardStatus dynamoDB config tableName shardId)
                    .Subscribe(function | NotProcessing 
                                            -> fetchNextRecords (SequenceNumber "") ShardIteratorType.TRIM_HORIZON (Iterator None)
                                               |> Async.StartImmediate
                                        | Processing(workerId', _, seqNum) when workerId' = workerId 
                                            -> fetchNextRecords seqNum ShardIteratorType.AFTER_SEQUENCE_NUMBER (Iterator None)
                                               |> Async.StartImmediate
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
            recordsSub.Dispose()
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
