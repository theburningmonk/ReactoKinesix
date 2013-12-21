namespace ReactoKinesix

open System
open System.Reactive
open System.Reactive.Linq
open System.Threading

open log4net

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
                            tableName  : TableName, 
                            streamName : StreamName,
                            workerId   : WorkerId,
                            shardId    : ShardId,
                            action     : Record -> unit) as this =
    let loggerName  = sprintf "ReactorKinesisWorker[Stream:%O, Worker:%O, Shard:%O]" streamName workerId shardId
    let logger      = LogManager.GetLogger(loggerName)
    let logDebug    = logDebug logger
    let logWarn     = logWarn  logger
    let logError    = logError logger

    do logDebug "Starting worker..." [||]

    let batchReceivedEvent          = new Event<Iterator * Record seq>()
    let recordProcessedEvent        = new Event<Record>()
    let processErroredEvent         = new Event<Record * Exception>()
    let batchProcessedEvent         = new Event<int * Iterator>()

    let startedEvent                = new Event<unit>()
    let emptyReceiveEvent           = new Event<unit>()
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
                                      logWarn "Failed to update heartbeat, ignoring..." [||]
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
                               | exn -> 
                                      // TODO : what's the right thing to do here if we failed to update checkpoint? 
                                      // a) keep going and risk allowing more records to be processed multiple times
                                      // b) crash and let the last batch of records be processed against
                                      // c) wait and recurse until we succeed until some other worker takes over
                                      //    processing of the shard in which case we get conditional check failed
                                      // for now, try option c) with a 1 second delay as the risk of processing the same
                                      // records can ony be determined by the consumer, perhaps expose some configurable
                                      // behaviour under these circumstances?
                                      logError exn "Failed to update checkpoint to [{0}]...retrying" [| seqNum |]
                                      do! Async.Sleep(1000)
                                      updateCheckpoint seqNum
        }
        Async.Start(work, cts.Token)

    let fetchNextRecords iterator = 
        async {
            logDebug "Fetching next records with iterator [{0}]" [| iterator |]

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

    let stopProcessing       = conditionalCheckFailedEvent.Publish
    let stopProcessingLogSub = stopProcessing.Subscribe(fun _ -> logDebug "Stop processing..." [||])
                         
    let heartbeat        = Observable.Interval(config.Heartbeat).TakeUntil(stopProcessing)
    let heartbeatLogSub  = heartbeat.Subscribe(fun _ -> logDebug "Sending heartbeat..." [||])
    let heartbeatSub     = heartbeat.Subscribe(updateHeartbeat)

    let checkpointLogSub = checkpointEvent.Publish.Subscribe(fun seqNum -> logDebug "Updating sequence number checkpoint [{0}]" [| seqNum |])
    
    let nextBatch       = startedEvent.Publish                            
                            .Merge(Observable.Delay(emptyReceiveEvent.Publish, config.EmptyReceiveDelay))
                            .Merge(checkpointEvent.Publish.Select(fun _ -> ()))

    let fetch           = batchProcessedEvent.Publish
                            .TakeUntil(stopProcessing)
    let fetchSub        = fetch.Subscribe(fun (_, iterator) -> fetchNextRecords iterator |> Async.StartImmediate)

    let received        = batchReceivedEvent.Publish
    let receivedLogSub  = received.Subscribe(fun (iterator, records) -> 
                            logDebug "Received batch of [{1}] records, next iterator [{0}]" [| iterator; Seq.length records|])

    let processing      = received
                            .Zip(nextBatch, fun receivedBatch _ -> receivedBatch)
                            .TakeUntil(stopProcessing)
    let processingLogSub = processing.Subscribe(fun (iterator, records) -> logDebug "Start processing batch of [{1}] records, next iterator [{0}]" [| iterator; Seq.length records |])
    let processingSub   = 
        processing.Subscribe(fun (iterator, records) ->
            match Seq.isEmpty records with
            | true -> 
                emptyReceiveEvent.Trigger()
                batchProcessedEvent.Trigger(0, iterator)
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
                    logDebug "Batch was fully processed [{0}], last sequence number [{1}]" [| count; seqNum |]

                    updateCheckpoint(seqNum)
                    batchProcessedEvent.Trigger(count, iterator)
                | Some (Failure (seqNum, _)) -> 
                    logWarn "Batch was partially processed [{0}/{1}], last successful sequence number [{2}]" 
                            [| count; Seq.length records; seqNum |]

                    updateCheckpoint(seqNum)
                    batchProcessedEvent.Trigger(count, NoIteratorToken <| AtSequenceNumber seqNum))

    let processedLogSub = recordProcessedEvent.Publish.Subscribe(fun (record : Record) ->
                            logDebug "Processed record [PartitionKey:{0}, SequenceNumber:{1}]"
                                     [| record.PartitionKey; record.SequenceNumber |])

    let initSub = Observable
                    .FromAsync(DynamoDBUtils.getShardStatus dynamoDB config tableName shardId)
                    .Subscribe(function 
                        | New(workerId', _) when workerId' = workerId
                            -> // the shard has not been processed before, so start from the oldest record
                               fetchNextRecords (NoIteratorToken <| TrimHorizon) |> Async.StartImmediate
                               startedEvent.Trigger()
                        | NotProcessing(_, _, seqNum)
                            -> // the shard has not been processed currently, start from the last checkpoint
                               fetchNextRecords (NoIteratorToken <| AfterSequenceNumber seqNum) |> Async.StartImmediate
                               startedEvent.Trigger()
                        | Processing(workerId', seqNum) when workerId' = workerId 
                            -> // the shard was being processed by this worker, continue from where we left off
                               fetchNextRecords (NoIteratorToken <| AfterSequenceNumber seqNum) |> Async.StartImmediate
                               startedEvent.Trigger()
                        | _ -> ())

    [<CLIEvent>] member this.OnBatchReceived            = batchReceivedEvent.Publish
    [<CLIEvent>] member this.OnRecordProcessed          = recordProcessedEvent.Publish
    [<CLIEvent>] member this.OnProcessErrored           = processErroredEvent.Publish
    [<CLIEvent>] member this.OnBatchProcessed           = batchProcessedEvent.Publish
    [<CLIEvent>] member this.OnEmptyReceive             = emptyReceiveEvent.Publish
    [<CLIEvent>] member this.OnCheckpoint               = checkpointEvent.Publish
    [<CLIEvent>] member this.OnHeartbeat                = heartbeatEvent.Publish
    [<CLIEvent>] member this.OnConditionalCheckFailed   = conditionalCheckFailedEvent.Publish

    interface IDisposable with
        member this.Dispose () = 
            if System.Threading.Interlocked.CompareExchange(disposeInvoked, 1, 0) = 0 then
                logDebug "Disposing..." [||]

                cts.Cancel()

                [| stopProcessingLogSub; heartbeatLogSub; heartbeatSub; checkpointLogSub; fetchSub;
                   receivedLogSub; processingLogSub; processingSub; processedLogSub; initSub; 
                   (cts :> IDisposable) |]
                |> Array.iter (fun x -> x.Dispose())

                logDebug "Disposed." [||]

type ReactoKinesixApp (awsKey     : string, 
                       awsSecret  : string, 
                       region     : RegionEndpoint,
                       appName    : string,
                       streamName : string,
                       ?config    : ReactoKinesixConfig) =
    let config = defaultArg config <| new ReactoKinesixConfig()
    do Utils.validateConfig config

    let loggerName = sprintf "ReactoKinesixApp[AppName:%s, Stream:%O]" appName streamName
    let logger     = LogManager.GetLogger(loggerName)
    let logDebug   = logDebug logger

    let disposeInvoked = ref 0

    let stateTableReadyEvent = new Event<string>()

    let kinesis    = AWSClientFactory.CreateAmazonKinesisClient(awsKey, awsSecret, region)
    let dynamoDB   = AWSClientFactory.CreateAmazonDynamoDBClient(awsKey, awsSecret, region) 
    
    let streamName = StreamName streamName

    let initResult = DynamoDBUtils.initStateTable dynamoDB config appName |> Async.RunSynchronously
    let tableName  = match initResult with 
                     | Success tableName -> tableName 
                     | Failure(_, exn)   -> raise <| InitializationFailed exn

    let tableReady = Observable.FromAsync(DynamoDBUtils.awaitStateTableReady dynamoDB tableName)

    // app is initialized when the app's state table is fully created
    let initializedLogSub = tableReady.Subscribe(fun _ -> logDebug "State table [{0}] is ready" [| tableName |])
    let initializedSub    = tableReady.Subscribe(fun _ -> stateTableReadyEvent.Trigger(tableName.ToString()))

    let start workerId action =
        let workerId  = WorkerId workerId

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

            let workers = createdShards |> Array.map (fun (shard, _) -> new ReactoKinesix(kinesis, dynamoDB, config, tableName, streamName, workerId, ShardId shard.ShardId, action))

            return { new IDisposable with member this.Dispose () = workers |> Array.iter (fun p -> (p :> IDisposable).Dispose()) }
        }
        |> Async.StartAsTask

    [<CLIEvent>] member this.OnStateTableReady = stateTableReadyEvent.Publish

    member this.Start(workerId : string, action : Action<Record>) = start workerId action.Invoke

    interface IDisposable with
        member this.Dispose () = 
            if System.Threading.Interlocked.CompareExchange(disposeInvoked, 1, 0) = 0 then
                logDebug "Disposing..." [||]

                initializedLogSub.Dispose()
                initializedSub.Dispose()

                logDebug "Disposed" [||]