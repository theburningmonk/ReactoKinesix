﻿namespace ReactoKinesix

open System
open System.Collections.Concurrent
open System.Collections.Generic
open System.Reactive
open System.Reactive.Linq
open System.Threading
open System.Threading.Tasks

open log4net

open Amazon
open Amazon.DynamoDBv2
open Amazon.DynamoDBv2.Model
open Amazon.Kinesis
open Amazon.Kinesis.Model

open ReactoKinesix.Model
open ReactoKinesix.Utils

type IRecordProcessor = 
    abstract member Process : Record -> unit

type IReactoKinesixApp =
    inherit IDisposable

    abstract member StartProcessing : shardId : string -> Task
    abstract member StopProcessing  : shardId : string -> Task
    abstract member ChangeProcessor : newProcessor : IRecordProcessor -> unit

type internal ReactoKinesix (app : ReactoKinesixApp, shardId : ShardId) as this =
    let loggerName  = sprintf "ReactorKinesixWorker[Stream:%O, Worker:%O, Shard:%O]" app.StreamName app.WorkerId shardId
    let logger      = LogManager.GetLogger(loggerName)
    let logDebug    = logDebug logger
    let logInfo     = logInfo  logger
    let logWarn     = logWarn  logger
    let logError    = logError logger

    do logDebug "Starting worker..." [||]

    let batchReceivedEvent          = new Event<Iterator * Record seq>() // when a new batch of records have been received
    let recordProcessedEvent        = new Event<Record>()                // when a record has been processed by processor
    let processErroredEvent         = new Event<Record * Exception>()    // when an error was caught when processing a record
    let batchProcessedEvent         = new Event<int * Iterator>()        // when a batch has finished processing with the iterator for next batch

    let initializedEvent            = new Event<unit>()           // when the worker has been initialized
    let initializationFailedEvent   = new Event<Exception>()      // when an error was caught whist initializing the worker
    
    let emptyReceiveEvent           = new Event<unit>()           // the worker received no records from the stream
    let checkpointEvent             = new Event<SequenceNumber>() // when the latest checkpoint is updated in state table
    let heartbeatEvent              = new Event<unit>()           // when a heartbeat is recorded
    let conditionalCheckFailedEvent = new Event<unit>()           // when a conditional check failure was encountered when writing to the state table

    let shardClosedEvent            = new Event<unit>() // when the shard is closed and will not return any more data (i.e. NextShardIterator is null)
    let stopProcessingEvent         = new Event<unit>() // when the worker is trying to stop processing of any further records
    let stoppedEvent                = new Event<unit>() // when the worker has completed stopped
    
    let disposeInvoked = ref 0
    let cts = new CancellationTokenSource();

    let updateHeartbeat _ = 
        let work = 
            async {
                logDebug "Sending heartbeat..." [||]

                let! res = DynamoDBUtils.updateHeartbeat app.DynamoDB app.TableName app.WorkerId shardId |> Async.Catch
                match res with
                | Choice1Of2 () -> heartbeatEvent.Trigger()
                | Choice2Of2 ex -> match ex with 
                                   | :? ConditionalCheckFailedException -> 
                                        conditionalCheckFailedEvent.Trigger()
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

    let rec updateIsClosed () =
        let work = 
            async {
                let! res = DynamoDBUtils.updateIsClosed true app.DynamoDB app.TableName app.WorkerId shardId |> Async.Catch
                match res with
                | Choice1Of2 ()  -> ()
                | Choice2Of2 exn -> match exn with
                                    | :? ConditionalCheckFailedException ->  
                                        conditionalCheckFailedEvent.Trigger()
                                    | _ -> logError exn "Failed to set IsClosed flag to true, retrying..." [||]
                                           updateIsClosed()
            }
        Async.Start(work, cts.Token)

    let rec updateCheckpoint seqNum = 
        let work = 
            async {
                let! res = DynamoDBUtils.updateCheckpoint seqNum app.DynamoDB app.TableName app.WorkerId shardId |> Async.Catch
                match res with
                | Choice1Of2 () -> logDebug "Updated sequence number checkpoint [{0}]" [| seqNum |]
                                   checkpointEvent.Trigger(seqNum)
                | Choice2Of2 ex -> match ex with 
                                   | :? ConditionalCheckFailedException -> 
                                        conditionalCheckFailedEvent.Trigger()
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

    let rec fetchNextRecords iterator = 
        async {
            match iterator with
            | EndOfShard -> shardClosedEvent.Trigger()
            | _ -> 
                logDebug "Fetching next records with iterator [{0}]" [| iterator |]

                let! getRecordsResult = KinesisUtils.getRecords app.Kinesis app.Config app.StreamName shardId iterator
                match getRecordsResult with
                | Success(nextIterator, batch) when nextIterator = null -> 
                    logDebug "Received batch of [{0}] records, no more records will be available (end of shard)" [| Seq.length batch |]
                    batchReceivedEvent.Trigger(EndOfShard, batch)
                | Success(nextIterator, batch) -> 
                    logDebug "Received batch of [{0}] records, next iterator [{1}]" [| Seq.length batch; iterator |]
                    batchReceivedEvent.Trigger(IteratorToken nextIterator, batch)
                | Failure(exn) -> 
                    match exn with
                    | :? ShardCannotBeIteratedException -> shardClosedEvent.Trigger()
                    | _ -> do! fetchNextRecords iterator
        }

    let processRecord record = 
        try 
            // NOTE: not sure why this type annotation is required to make the type inference happy..
            (app.Processor :> IRecordProcessor).Process(record)

            logDebug "Processed record [PartitionKey:{0}, SequenceNumber:{1}]"
                     [| record.PartitionKey; record.SequenceNumber |]

            recordProcessedEvent.Trigger(record)
            Success(SequenceNumber record.SequenceNumber)
        with
        | ex -> 
            processErroredEvent.Trigger(record, ex)
            Failure(SequenceNumber record.SequenceNumber, ex)

    let processBatch (iterator, records) =
        logDebug "Start processing batch of [{1}] records, next iterator [{0}]" [| iterator; Seq.length records |]

        match Seq.isEmpty records with
        | true -> 
            emptyReceiveEvent.Trigger()
            batchProcessedEvent.Trigger(0, iterator)
        | _ -> 
            // keep processing the records until the first error
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
                batchProcessedEvent.Trigger(count, NoIteratorToken <| AtSequenceNumber seqNum)

    let onShardClosed _ =
        logInfo "Shard is closed, no more records will be available." [||]                    
        updateIsClosed()
        app.MarkAsClosed(shardId)
        stoppedEvent.Trigger()

    // stop processing anymore records if we encountered conditional check errors (indicative of another worker having taken over
    // control of the shard from us), or if we were explicitly told to stop (indicated by the stoppingProcessingEvent)
    let stopProcessing = stopProcessingEvent.Publish
                            .Merge(conditionalCheckFailedEvent.Publish)
                            .Take(1)
    let _ = stopProcessing.Subscribe(fun _ -> logInfo "Stopping..." [||])

    let stopped = stoppedEvent.Publish.Take(1)
    let _       = stopped.Subscribe(fun _ -> 
                    logInfo "Processing has stopped." [||]
                    (this :> IDisposable).Dispose())

    let _       = Observable
                    .Interval(app.Config.Heartbeat)
                    .TakeUntil(stopped)
                    .Subscribe(updateHeartbeat)

    let checkpoint  = checkpointEvent.Publish.Select(fun _ -> ())

    // signal to process the next batch of records that has been received
    let nextBatch   = initializedEvent.Publish
                        .Merge(Observable.Delay(emptyReceiveEvent.Publish, app.Config.EmptyReceiveDelay))
                        .Merge(checkpoint)
                        .TakeUntil(stopProcessing)

    let processed   = batchProcessedEvent.Publish

    // fetch new records after the previous batch has been processed until we need to either stop processing or the shard is closed
    let stopFetching    = stopProcessing.Merge(shardClosedEvent.Publish)

    let fetch           = processed.TakeUntil(stopFetching)
    let _               = fetch.Subscribe(fun (_, iterator) -> Async.StartImmediate(fetchNextRecords iterator, cts.Token))

    let received        = batchReceivedEvent.Publish

    // after we have received the next batch of records, wait for the nextBatch signal.
    // this could include a forced period of delay if the last batch was empty even if this batch is not empty, this is so that
    // we don't spam Kinesis with too many calls when there are no records to process
    let processing      = received
                            .Zip(nextBatch, fun receivedBatch _ -> receivedBatch)
                            .TakeUntil(stopProcessing)
    let processingSub   = processing.Subscribe(fun args -> processBatch args)

    // only deal with the shard closed event the first time we see it
    let _ = shardClosedEvent.Publish.Take(1).Subscribe(onShardClosed)

    // when the stop is triggered during a current batch then the next checkpoint/empty receive event tells us the current batch is 
    // finished so we can trigger the stopped event, e.g.
    //      ---processing---stop----processed----checkpoint (when records were received)
    //      ---processing-------processed--stop--checkpoint (when records were received)
    //      ---processing---stop--empty receive--processed  (when no records were received)
    // if the stop came between empty receive and processed then it'll be handled by the next case.
    //
    // when the stop is triggered when fetching records then the next receive event will suffice as signal that it's safe to
    // assume that we can trigger the stopped event since the processing stream will not proceed now that stop has been triggered
    //      processed---stop---received
    //                fetching
    //
    // hence the stop points consist of checkpoint, empty receive and received
    let stopPoints = checkpoint
                        .Merge(emptyReceiveEvent.Publish)
                        .Merge(received.Select(fun _ -> ()))
    let _ = stopProcessing
                .CombineLatest(stopPoints, fun _ _ -> ())
                .Take(1)
                .Subscribe(fun _ -> stoppedEvent.Trigger())

    // this is the initialization sequence
    let init = 
        let getIterator = function
            | Some seqNum -> NoIteratorToken <| AfterSequenceNumber seqNum
            | _ -> NoIteratorToken TrimHorizon            

        async {
            let! createShardResult = DynamoDBUtils.createShard app.DynamoDB app.TableName app.WorkerId shardId
            match createShardResult with
            | Failure exn -> initializationFailedEvent.Trigger(exn)
            | Success _   -> 
                let! getShardStatusResult = DynamoDBUtils.getShardStatus app.DynamoDB app.Config app.TableName shardId
                match getShardStatusResult with
                | Failure exn -> initializationFailedEvent.Trigger(exn)
                | Success status -> 
                    match status with
                    | NotFound -> 
                        logWarn "Shard is not found. Please check if it was manually deleted from DynamoDB." [||]
                        initializationFailedEvent.Trigger(ShardNotFoundException)
                    | Closed   -> shardClosedEvent.Trigger()
                    | NotProcessing(workerId', heartbeat, seqNum) -> 
                        logDebug "Taking over shard which was processed by worker [{0}], last heartbeat [{1}] and checkpoint [{2}]"
                                 [| workerId'; heartbeat; seqNum |]

                        // claim ownership of the shard by successfully updating the row
                        try
                            do! DynamoDBUtils.updateWorkerId app.WorkerId app.DynamoDB app.TableName workerId' shardId

                            logDebug "Successfully taken over responsibility for the shard" [||]

                            // the shard has not been processed currently, start from the last checkpoint
                            Async.Start(fetchNextRecords <| getIterator seqNum, cts.Token)
                            initializedEvent.Trigger()
                        with
                        | :? ConditionalCheckFailedException ->
                            conditionalCheckFailedEvent.Trigger()
                        | exn -> initializationFailedEvent.Trigger(exn)

                    | Processing(workerId', seqNum) when workerId' = app.WorkerId -> 
                        logDebug "Resuming processing of shard, last checkpoint [{0}]" [| seqNum |]

                        // the shard was being processed by this worker, continue from where we left off
                        Async.Start(fetchNextRecords <| getIterator seqNum, cts.Token)
                        initializedEvent.Trigger()
                    | Processing(workerId', seqNum) ->
                        // the shard is being processed by another worker, for now, give up
                        logDebug "Shard is currently being processed by worker [{0}], last checkpoint [{1}], retiring." [| workerId'; seqNum |]
                        stoppedEvent.Trigger()
        }

    // keep retrying failed initializations until it succeeds
    let retryInitSub = initializationFailedEvent.Publish
                        .TakeUntil(initializedEvent.Publish)
                        .Subscribe(fun _ -> Async.Start(init, cts.Token))

    do Async.Start(init, cts.Token)

    let cleanup (disposing : bool) =
        // ensure that resources are only disposed of once
        if System.Threading.Interlocked.CompareExchange(disposeInvoked, 1, 0) = 0 then
            logDebug "Disposing..." [||]

            cts.Cancel()

            [| processingSub; retryInitSub; (cts :> IDisposable) |]
            |> Array.iter (fun x -> x.Dispose())

            logDebug "Disposed." [||]

    [<CLIEvent>] member this.OnStopped = stoppedEvent.Publish

    member this.Stop () = 
        stopProcessingEvent.Trigger()
        stoppedEvent.Publish.Wait()

    interface IDisposable with
        member this.Dispose () = 
            GC.SuppressFinalize(this)
            cleanup(true)

    // provide a finalizer so that in the case the consumer forgets to dispose of the worker the
    // finalizer will clean up
    override this.Finalize () =
        logWarn "Finalizer is invoked. Please ensure that the object is disposed in a deterministic manner instead." [||]
        cleanup(false)

and ReactoKinesixApp private (awsKey     : string, 
                              awsSecret  : string, 
                              region     : RegionEndpoint,
                              appName    : string,
                              streamName : string,
                              workerId   : string,
                              processor  : IRecordProcessor,
                              config     : ReactoKinesixConfig) as this =
    // track a static dictionary of application names that are currenty running to prevent
    // consumer from accidentally starting multiple apps with same name
    static let runningApps = new ConcurrentDictionary<string, string>()        
    do if not <| runningApps.TryAdd(appName, streamName) 
       then raise <| AppNameIsAlreadyRunningException streamName
    
    do Utils.validateConfig config

    let loggerName = sprintf "ReactoKinesixApp[AppName:%s, Stream:%O]" appName streamName
    let logger     = LogManager.GetLogger(loggerName)
    let logDebug   = logDebug logger
    let logInfo    = logInfo  logger
    let logWarn    = logWarn  logger

    let cts = new CancellationTokenSource()

    let stateTableReadyEvent = new Event<string>()

    let kinesis    = AWSClientFactory.CreateAmazonKinesisClient(awsKey, awsSecret, region)
    let dynamoDB   = AWSClientFactory.CreateAmazonDynamoDBClient(awsKey, awsSecret, region)    
    let streamName, workerId = StreamName streamName, WorkerId workerId
    let tableName            = TableName <| sprintf "%s%s" appName config.DynamoDBTableSuffix
    let mutable processor    = processor

    let initTable () = 
       match DynamoDBUtils.initStateTable dynamoDB config tableName with
       | Success _   -> ()
       | Failure exn -> raise <| InitializationFailedException exn
    do initTable |> withRetry 3

    // this is a mutable dictionary of workers but can only be mutated from within the controller agent
    // which is single threaded by nature so there's no need for placing locks around add/remove operations
    let knownShards = new Dictionary<ShardId, bool>()
    let workers = new Dictionary<ShardId, ReactoKinesix>()
    let body (inbox : Agent<ControlMessage>) = 
        async {
            while true do
                let! msg = inbox.Receive()

                match msg with
                | StartWorker(shardId, reply) ->
                    match workers.TryGetValue(shardId) with
                    | true, worker -> reply.Reply()
                    | _ -> let worker = new ReactoKinesix(this, shardId)
                           workers.Add(shardId, worker)
                           worker.OnStopped.Add(fun _ -> inbox.Post(RemoveWorker(shardId)))
                           reply.Reply()
                | StopWorker(shardId, reply) -> 
                    match workers.TryGetValue(shardId) with
                    | true, worker -> 
                        async {
                            worker.Stop()
                            reply.Reply()
                        }
                        |> Async.Start
                        
                    | _ -> reply.Reply()
                | RemoveWorker(shardId) -> workers.Remove(shardId) |> ignore
                | AddKnownShard(shardId, reply) ->
                    if not <| knownShards.ContainsKey(shardId) then knownShards.Add(shardId, false)
                    reply.Reply()
                | MarkAsClosed(shardId, reply) ->
                    knownShards.[shardId] <- true
                    reply.Reply()
        }
    let controller = Agent<ControlMessage>.StartProtected(body, cts.Token, onRestart = fun exn -> logWarn "Controller agent was restarted due to exception :\n {0}" [| exn |])

    let startWorker shardId   = controller.PostAndAsyncReply(fun reply -> StartWorker(shardId, reply))
    let stopWorker  shardId   = controller.PostAndAsyncReply(fun reply -> StopWorker(shardId, reply))
    let addKnownShard shardId = controller.PostAndAsyncReply(fun reply -> AddKnownShard(shardId, reply))
    let markAsClosed shardId  = controller.PostAndAsyncReply(fun reply -> MarkAsClosed(shardId, reply))

    let updateWorkers (shardIds : string seq) (update : ShardId -> Async<unit>) = 
        async {
            do! shardIds                
                |> Seq.map (fun shardId -> update (ShardId shardId))
                |> Async.Parallel
                |> Async.Ignore
        }

    let refresh =
        async {
            // find difference between the shards in the stream and the shards we're currenty processing
            let! shards  = KinesisUtils.getShards kinesis streamName
            let shardIds = shards |> Seq.map (fun shard -> shard.ShardId) |> Set.ofSeq

            let knownShards = knownShards.Keys |> Seq.map (fun (ShardId shardId) -> shardId) |> Set.ofSeq
            let newShards   = Set.difference shardIds knownShards

            if newShards.Count > 0 then
                let logArgs : obj[] = [| newShards.Count; String.Join(",", newShards) |]
                logInfo "Add [{0}] shards to known shards : [{1}]" logArgs
                do! updateWorkers newShards addKnownShard

                logInfo "Starting workers for [{0}] shards : [{1}]" logArgs
                do! updateWorkers newShards startWorker
       }
       
    let _ = Observable.FromAsync(DynamoDBUtils.awaitStateTableReady dynamoDB tableName)
                      .Subscribe(fun _ -> 
                            stateTableReadyEvent.Trigger(tableName.ToString())
                            logDebug "State table [{0}] is ready, initializing workers..." [| tableName |]
                            Async.Start(refresh, cts.Token))

    let refreshSub = Observable.Interval(config.CheckStreamChangesFrequency)
                        .Subscribe(fun _ -> Async.Start(refresh, cts.Token))
    
    let disposeInvoked = ref 0
    let cleanup (disposing : bool) =
        // ensure that resources are only disposed of once
        if System.Threading.Interlocked.CompareExchange(disposeInvoked, 1, 0) = 0 then
            logDebug "Disposing..." [||]

            refreshSub.Dispose()

            workers.Keys 
            |> Seq.toArray 
            |> Seq.map (fun shardId -> stopWorker shardId)
            |> Async.Parallel
            |> Async.Ignore
            |> Async.RunSynchronously

            cts.Cancel()
            cts.Dispose()

            runningApps.TryRemove(appName) |> ignore

            logDebug "Disposed" [||]

    member internal this.Kinesis    = kinesis
    member internal this.DynamoDB   = dynamoDB
    member internal this.Config     = config
    member internal this.TableName  = tableName
    member internal this.StreamName = streamName
    member internal this.WorkerId   = workerId
    member internal this.Processor  = processor
    
    member internal this.MarkAsClosed shardId   = markAsClosed shardId |> ignore
    member internal this.StopProcessing shardId = stopWorker shardId   |> ignore

    static member CreateNew(awsKey, awsSecret, region, appName, streamName, workerId, processor, ?config) =
        let config = defaultArg config <| new ReactoKinesixConfig()
        new ReactoKinesixApp(awsKey, awsSecret, region, appName, streamName, workerId, processor, config) :> IReactoKinesixApp

    interface IReactoKinesixApp with
        member this.StartProcessing (shardId : string) = startWorker (ShardId shardId) |> Async.StartAsPlainTask
        member this.StopProcessing  (shardId : string) = stopWorker  (ShardId shardId) |> Async.StartAsPlainTask
        member this.ChangeProcessor newProcessor       = processor <- newProcessor

    interface IDisposable with
        member this.Dispose () = 
            GC.SuppressFinalize(this)
            cleanup(true)

    // provide a finalizer so that in the case the consumer forgets to dispose of the app the
    // finalizer will clean up
    override this.Finalize () =
        logWarn "Finalizer is invoked. Please ensure that the object is disposed in a deterministic manner instead." [||]
        cleanup(false)