namespace ReactoKinesix

open System
open System.Reactive
open System.Reactive.Linq
open System.Threading

open Amazon
open Amazon.DynamoDBv2
open Amazon.DynamoDBv2.Model
open Amazon.Kinesis
open Amazon.Kinesis.Model

open ReactoKinesix.Model
open ReactoKinesix.Utils

type private ReactoKinesix (kinesis    : IAmazonKinesis,
                            dynamoDB   : IAmazonDynamoDB,
                            config     : ReactoKinesixConfig,
                            tableName, 
                            streamName,
                            workerId,
                            shardId,
                            action     : Record -> unit) as this =
    let batchReceivedEvent          = new Event<Iterator * Record seq>()
    let recordProcessedEvent        = new Event<Record>()
    let processErroredEvent         = new Event<Record * Exception>()
    let batchProcessedEvent         = new Event<int * Iterator>()

    let checkpointEvent             = new Event<SequenceNumber>()
    let heartbeatEvent              = new Event<unit>()
    let conditionalCheckFailedEvent = new Event<unit>()

    let disposeInvoked = ref 0
    let dispose () = (this :> IDisposable).Dispose()

    let cts = new CancellationTokenSource();

    let updateHeartbeat _ = 
        let work = async {
            let! res = DynamoDBUtils.updateHeartbeat dynamoDB tableName workerId shardId |> Async.Catch
            match res with
            | Choice1Of2 () -> heartbeatEvent.Trigger()
            | Choice2Of2 ex -> match ex with 
                               | :? ConditionalCheckFailedException -> 
                                    conditionalCheckFailedEvent.Trigger()
                                    dispose()
                               | _ -> // TODO : what's the right thing to do here?
                                      // a) give up, let the next cycle (or next checkpoint update) update the heartbeat
                                      // b) retry a few times
                                      // c) retry until we succeed
                                      // for now, try option a) as it's not entirely critical for one heartbeat update to
                                      // succeed, if problem is with DynamoDB and it persists then eventually we'll be
                                      // blocked on the checkpoint update too and either succeed eventualy or some other
                                      // worker will take over if they were able to successful write to DynamoDB instead
                                      // of the current worker
                                      ()
        }
        Async.Start(work, cts.Token)

    let rec updateCheckpoint seqNum =
        let work = async {
            let! res = DynamoDBUtils.updateCheckpoint seqNum dynamoDB tableName workerId shardId |> Async.Catch
            match res with
            | Choice1Of2 () -> checkpointEvent.Trigger(seqNum)
            | Choice2Of2 ex -> match ex with 
                               | :? ConditionalCheckFailedException -> 
                                    conditionalCheckFailedEvent.Trigger()
                                    dispose()
                               | _ -> // TODO : what's the right thing to do here if we failed to update checkpoint? 
                                      // a) keep going and risk allowing more records to be processed multiple times
                                      // b) crash and let the last batch of records be processed against
                                      // c) wait and recurse until we succeed until some other worker takes over
                                      //    processing of the shard in which case we get conditional check failed
                                      // for now, try option c) with a 1 second delay as the risk of processing the same
                                      // records can ony be determined by the consumer, perhaps expose some configurable
                                      // behaviour under these circumstances?
                                      do! Async.Sleep(1000)
                                      updateCheckpoint seqNum
        }
        Async.Start(work, cts.Token)

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
                            .Zip(checkpointEvent.Publish, fun batchProcessedRes _ -> batchProcessedRes)
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
                    | Some (Success seqNum)      -> 
                        updateCheckpoint(seqNum)
                        batchProcessedEvent.Trigger(count, iterator)
                    | Some (Failure (seqNum, _)) -> 
                        updateCheckpoint(seqNum)
                        batchProcessedEvent.Trigger(count, NoIteratorToken <| AtSequenceNumber seqNum))

    let initSub = Observable
                    .FromAsync(DynamoDBUtils.getShardStatus dynamoDB config tableName shardId)
                    .Subscribe(function 
                        | New(workerId', _) when workerId' = workerId
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
    [<CLIEvent>] member this.OnCheckpoint               = checkpointEvent.Publish
    [<CLIEvent>] member this.OnHeartbeat                = heartbeatEvent.Publish
    [<CLIEvent>] member this.OnConditionalCheckFailed   = conditionalCheckFailedEvent.Publish

    interface IDisposable with
        member this.Dispose () = 
            if System.Threading.Interlocked.CompareExchange(disposeInvoked, 1, 0) = 0 then
                cts.Cancel()
                heartbeatSub.Dispose()
                fetchSub.Dispose()
                receivedSub.Dispose()
                initSub.Dispose()

type ReactoKinesixApp (awsKey     : string, 
                       awsSecret  : string, 
                       region     : RegionEndpoint,
                       appName    : string,
                       streamName : string,
                       ?config    : ReactoKinesixConfig) =
    let config = defaultArg config <| new ReactoKinesixConfig()
    do Utils.validateConfig config

    let stateTableCreatedEvent      = new Event<TableName>()

    let kinesis    = AWSClientFactory.CreateAmazonKinesisClient(awsKey, awsSecret, region)
    let dynamoDB   = AWSClientFactory.CreateAmazonDynamoDBClient(awsKey, awsSecret, region) 
    
    let streamName = StreamName streamName

    let tableCreated = Observable.FromAsync(DynamoDBUtils.initStateTable dynamoDB config appName)
    let tableReady   = tableCreated.SelectMany(fun tableName -> Observable.FromAsync(DynamoDBUtils.awaitStateTableReady dynamoDB tableName))

    // app is initialized when the app's state table is fully created
    let initialized  = tableCreated.Zip(tableReady, fun tableName _ -> tableName)    

    let start workerId action =
        let workerId  = WorkerId workerId

        // wait for the app to be initialized
        let tableName = initialized.Wait()

        async {
            let! shards = KinesisUtils.getShards kinesis streamName
            let! createdShards = 
                shards
                |> Seq.map (fun shard -> 
                    async { 
                        let! isCreated = DynamoDBUtils.createShard dynamoDB tableName workerId (ShardId shard.ShardId)
                        return shard, isCreated
                    })
                |> Async.Parallel

            let workers = createdShards 
                            |> Array.filter snd 
                            |> Array.map (fun (shard, _) -> new ReactoKinesix(kinesis, dynamoDB, config, tableName, streamName, workerId, ShardId shard.ShardId, action))

            return { new IDisposable with member this.Dispose () = workers |> Array.iter (fun p -> (p :> IDisposable).Dispose()) }
        }
        |> Async.StartAsTask

    member this.Start(workerId : string, action : Action<Record>) = start workerId action.Invoke