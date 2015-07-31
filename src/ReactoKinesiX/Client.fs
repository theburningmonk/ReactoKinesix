namespace ReactoKinesix

open System
open System.Collections.Concurrent
open System.Collections.Generic
open System.Linq
open System.Reactive
open System.Reactive.Linq
open System.Reactive.Subjects
open System.Threading
open System.Threading.Tasks
open FSharp.Reactive

open log4net

open Amazon
open Amazon.CloudWatch
open Amazon.DynamoDBv2
open Amazon.DynamoDBv2.Model
open Amazon.Kinesis
open Amazon.Kinesis.Model

open ReactoKinesix.Model
open ReactoKinesix.Utils

/// Represents a processor that is responsible for processing any records received from the stream.
type IRecordProcessor = 
    inherit IDisposable

    /// Process a batch of record
    abstract member Process             : shardId : string * records : Record[] -> ProcessRecordsResult

    /// This method is called when we have exceeded the number of retries for a record
    abstract member OnMaxRetryExceeded  : records : Record[] * errorHandlingMode : ErrorHandlingMode -> unit

/// Factory for record processor
type IRecordProcessorFactory =
    /// Creates a new record processor for a shard with the specified shard ID
    abstract member CreateNew           : shardId : string -> IRecordProcessor

/// Represents a client application that consumes records from a Kinesis stream.
/// Please use the static method "ReactoKinesixApp.CreateNew(...)" to create a new Kinesis client application.
type IReactoKinesixApp =
    inherit IDisposable

    /// Fired when the application has been initialized
    [<CLIEvent>]
    abstract member OnInitialized           : IEvent<OnInitializedDelegate, EventArgs>

    /// Fired when a batch of records have been successfully processed
    [<CLIEvent>]
    abstract member OnBatchProcessed        : IEvent<OnBatchProcessedDelegate, EventArgs>

    /// Force the application to try and start processing a particular shard
    abstract member StartProcessing         : shardId : string -> Task

    /// Force the application to stop processing a particuar shard
    abstract member StopProcessing          : shardId : string -> Task

    /// Change the processor that will be used to process received records
    abstract member ChangeProcessorFactory  : newFactory : IRecordProcessorFactory -> unit

type internal ProcessorFactory = ShardId -> IRecordProcessor

type internal WorkingShard =
    | Started   of ReactoKinesixShardProcessor
    | Stopped   of StoppedReason

and ReactoKinesixApp private (kinesis           : IAmazonKinesis, 
                              dynamoDB          : IAmazonDynamoDB, 
                              cloudWatch        : IAmazonCloudWatch,
                              appName           : string,
                              streamName        : string,
                              workerId          : string,
                              processorFactory  : IRecordProcessorFactory,
                              config            : ReactoKinesixConfig) as this =
    // track a static dictionary of application names that are currenty running to prevent
    // consumer from accidentally starting multiple apps with same name
    static let runningApps = new ConcurrentDictionary<string, string>()
    do if not <| runningApps.TryAdd(appName, streamName) 
       then raise <| AppNameIsAlreadyRunningException streamName
    
    do Utils.validateConfig config

    let loggerName = sprintf "ReactoKinesixApp[AppName:%s, Stream:%s]" appName streamName
    let logger     = LogManager.GetLogger(loggerName)
    let logDebug   = logDebug logger
    let logInfo    = logInfo  logger
    let logWarn    = logWarn  logger
    let logError   = logError logger

    let cts = new CancellationTokenSource()

    let initializedEvent                = new Event<OnInitializedDelegate, EventArgs>()
    let batchProcessedEvent             = new Event<OnBatchProcessedDelegate, EventArgs>()
    let shardProcessorCountChangedEvent = new Event<int>()  // when the number of shard processors have changed
         
    let mutable processorFactory        = processorFactory

    let streamName, workerId     = StreamName streamName, WorkerId workerId
    let tableName                = TableName <| sprintf "%s%s" appName config.DynamoDBTableSuffix
    let metricsAgent             = new MetricsAgent(cloudWatch, appName, streamName)
    
    let runScheduledTask freq job = 
        let wrapped = job |> Async.Catch |> Async.Ignore
        Observable
            .Interval(freq)
            .Subscribe(fun _ -> 
                Async.StartImmediate(wrapped, cancellationToken = cts.Token))
        
    let updateShardProcessors (shardIds : string seq) (update : ShardId -> Async<unit>) = 
        async {
            do! shardIds
                |> Seq.map (fun shardId -> update (ShardId shardId))
                |> Async.Parallel
                |> Async.Ignore
        }

    //#region Initialization

    let initTable () = 
       match DynamoDBUtils.initStateTable dynamoDB config tableName with
       | Success _   -> ()
       | Failure exn -> raise <| InitializationFailedException exn
    do initTable |> withRetry 3

    //#endregion

    //#region Agent that tracks known shards and shard processors

    // this is a mutable dictionary of shard processor but can only be mutated from within the controller agent
    // which is single threaded by nature so there's no need for placing locks around add/remove operations
    let knownShards   = new Dictionary<ShardId, bool>()
    let workingShards = new Dictionary<ShardId, WorkingShard>()
    let getShardProcessorCount () = workingShards.Values |> Seq.filter (function | Started _ -> true | _ -> false) |> Seq.length
    let body (inbox : Agent<ControlMessage>) = 
        async {
            while true do
                let! msg = inbox.Receive()

                match msg with
                | StartShardProcessor(shardId, reply) ->
                    match workingShards.TryGetValue(shardId) with
                    | true, Started _ -> reply.Reply()
                    | _ -> let shardProcessor = new ReactoKinesixShardProcessor(this, shardId)
                           workingShards.[shardId] <- Started shardProcessor
                           shardProcessor.OnStopped.Add(fun reason -> inbox.Post <| RemoveShardProcessor(shardId, reason))
                           shardProcessor.OnBatchProcessed.Add(fun _ -> batchProcessedEvent.Trigger(this, new EventArgs()))
                           reply.Reply()
                           shardProcessorCountChangedEvent.Trigger <| getShardProcessorCount()
                | StopShardProcessor(shardId, reply) -> 
                    match workingShards.TryGetValue(shardId) with
                    | true, Started shardProcessor -> 
                        shardProcessor.Stop()
                        reply.Reply()
                    | _ -> reply.Reply()
                | RemoveShardProcessor(shardId, reason) -> 
                    workingShards.[shardId] <- Stopped reason
                    shardProcessorCountChangedEvent.Trigger <| getShardProcessorCount()
                | AddKnownShard(shardId, reply) ->
                    if not <| knownShards.ContainsKey(shardId) then knownShards.Add(shardId, false)
                    reply.Reply()
                | MarkAsClosed(shardId, reply) ->
                    knownShards.[shardId] <- true
                    reply.Reply()
                | RemoveKnownShard(shardId, reply) ->
                    knownShards.Remove(shardId) |> ignore
                    reply.Reply()
        }

    let onControllerRestart exn = logWarn exn "Controller agent was restarted due to exception" [||]
    let controller = 
        Agent<ControlMessage>.StartProtected(
            body, 
            cts.Token, 
            onRestart = onControllerRestart)

    let startShardProcessor shardId = controller.PostAndAsyncReply(fun reply -> StartShardProcessor(shardId, reply))
    let stopShardProcessor  shardId = controller.PostAndAsyncReply(fun reply -> StopShardProcessor(shardId, reply))
    let addKnownShard shardId = controller.PostAndAsyncReply(fun reply -> AddKnownShard(shardId, reply))
    let rmvKnownShard shardId = controller.PostAndAsyncReply(fun reply -> RemoveKnownShard(shardId, reply))
    let markAsClosed shardId  = controller.PostAndAsyncReply(fun reply -> MarkAsClosed(shardId, reply))

    //#endregion

    //#region Monitor sharding changes in the stream

    // look for changes to the stream compared to the shards we know about
    let checkStreamChanges =
        async {
            // find difference between the shards in the stream and the shards we're currenty processing
            let! res = KinesisUtils.getShards kinesis streamName

            match res with
            | Success shards ->
                let shardIds = shards |> Seq.map (fun shard -> shard.ShardId) |> Set.ofSeq

                let knownShards = knownShards.Keys |> Seq.map (fun (ShardId shardId) -> shardId) |> Set.ofSeq
                let newShards   = Set.difference shardIds knownShards
                let rmvShards   = Set.difference knownShards shardIds

                if newShards.Count > 0 then
                    let logArgs : obj[] = [| newShards.Count; csv newShards |]
                    logInfo "Add [{0}] shards to known shards : [{1}]" logArgs
                    do! updateShardProcessors newShards addKnownShard

                    logInfo "Starting shard processors for [{0}] shards : [{1}]" logArgs
                    do! updateShardProcessors newShards startShardProcessor

                if rmvShards.Count > 0 then
                    logInfo "Remove [{0}] shards from known shards : [{1}]" [| rmvShards.Count; csv rmvShards |]
                    do! updateShardProcessors newShards rmvKnownShard
            | Failure exn -> logWarn exn "Failed to retrieve information about shards, skipping check for stream changes..." [||]
        }
       
    let _ = Observable
                .FromAsync(DynamoDBUtils.awaitStateTableReady dynamoDB tableName)
                .Subscribe(fun _ -> 
                    initializedEvent.Trigger(this, new EventArgs())
                    logDebug "State table [{0}] is ready, initializing shard processors..." [| tableName |]
                    Async.StartImmediate(checkStreamChanges, cancellationToken = cts.Token))

    let refreshSub = runScheduledTask config.CheckStreamChangesFrequency checkStreamChanges

    //#endregion

    //#region Monitor shards that aren't processed by any worker

    // look for shards that are not being processed by any worker
    let checkUnprocessed =
        async {
            // only check against shards that was:
            //  * processed by other worker
            //  * stopped due to conditional check failure (indicative of the shard being taken over by another worker)
            let shardsToCheck = workingShards 
                                |> Seq.choose (fun (KeyValue(ShardId shardId, workingShard)) -> 
                                        match workingShard with 
                                        | Stopped StoppedReason.ProcessedByOther
                                        | Stopped StoppedReason.ConditionalCheckFailed
                                        | Stopped StoppedReason.HandedOver
                                            -> Some shardId
                                        | _ -> None) 
                                |> Seq.toArray

            if shardsToCheck.Length > 0 then
                let logArgs : obj[] = [| shardsToCheck.Length; shardsToCheck |]
                logInfo "Found [{0}] shards that are not closed and not processed by this worker : [{1}]" logArgs                        
                logInfo "Starting shard processors for [{0}] shards : [{1}]" logArgs
                do! updateShardProcessors shardsToCheck startShardProcessor
        }

    let checkUnprocessedSub = runScheduledTask config.CheckUnprocessedShardsFrequency checkUnprocessed

    //#endregion

    //#region Managing load balancing

    let issueHandoverRequest fromWorkerId shardId =
        async {
            let! res = DynamoDBUtils.issueHandoverRequest dynamoDB config tableName fromWorkerId workerId shardId
            match res with
            | Choice1Of2 ()  -> logDebug "Issued handover request to worker [{0}] for shard [{1}]" [| fromWorkerId; shardId |]
            | Choice2Of2 exn -> logError exn "Failed to issued handover request to worker [{0}] for shard [{1}]" [| fromWorkerId; shardId |]
        }

    // look for workers whom have taken on too many shards and request to share their load to balance the load amongst the workers
    let shareLoad =
        async {
            let! shardStatuses = DynamoDBUtils.getShardStatuses dynamoDB config tableName
            match shardStatuses with
            | Success statuses 
                when statuses |> Array.exists (function 
                        | NotProcessing _ | HandingOver _ | Processing (_, _, _, Some _) -> true 
                        | _ -> false) ->
                    logDebug "There are shards not currently being processed in the stream, skip attempt to share load until all shards are being processed" [||]
            | Success statuses ->
                let shardsCount    = statuses.Length
                let shardsByWorker = statuses 
                                     |> Seq.groupBy (function | Processing(_, workerId', _, _)  -> workerId')
                                     |> Map.ofSeq
                let shardsCountByWorker = shardsByWorker
                                          |> Seq.map (fun (KeyValue(workerId', seq)) -> workerId', Seq.length seq)
                                          |> Seq.toArray
                let myShardsCount  = shardsCountByWorker 
                                     |> Array.tryPick (fun (workerId', count) -> if workerId' = workerId then Some count else None)
                                     |> function | Some n -> n | _ -> 0               

                // look for workers with at least 2 more shards than the current worker
                let workersWithMoreLoad = shardsCountByWorker |> Array.filter (fun (_, count) -> count > myShardsCount + 1)

                match workersWithMoreLoad with
                | [||] -> () // no one's got more load, so do nothing
                | workersWithMoreLoad ->
                    // whilst trying to ask other workers to hand over their shards, only do so as far as not to give the
                    // current worker more than the current worker
                    let maxTargetShards = workersWithMoreLoad |> Seq.map snd |> Seq.min
                    let shardsToTake    = min (maxTargetShards - myShardsCount) workersWithMoreLoad.Length
                
                    // these are the workers whom we're going to ask to hand over one of their shards
                    let targetWorkers = workersWithMoreLoad 
                                        |> Seq.sortBy (fun (_, count) -> -count)
                                        |> Seq.take shardsToTake
                                        |> Seq.map fst
                                        |> Seq.toArray

                    logDebug "Attempting to request [{0}] workers to handover one of their shards : [{1}]" 
                             [| targetWorkers.Length; targetWorkers |> Seq.map (function WorkerId workerId -> workerId) |> csv |]
                
                    targetWorkers 
                    |> Array.iter (fun fromWorkerId -> 
                        let shard = shardsByWorker.[fromWorkerId] |> Seq.head
                        Async.Start(issueHandoverRequest fromWorkerId shard.ShardId, cts.Token))
            | Failure exn ->
                logError exn "Failed to retrieve shard statuses from the state table [{0}]" [| tableName |]
        }

    let shareLoadSub = runScheduledTask config.LoadBalanceFrequency shareLoad
    let proactiveShareLoadSub = 
        shardProcessorCountChangedEvent.Publish
            .Where((=) 0)
            .Subscribe(fun _ -> Async.StartImmediate(shareLoad, cts.Token))
    
    //#endregion

    let shardProcessorCountSub = shardProcessorCountChangedEvent.Publish.Subscribe(fun n -> logDebug "Shard Processor count changed to [{0}]" [| n |])

    let disposeInvoked = ref 0
    let cleanup (disposing : bool) =
        // ensure that resources are only disposed of once
        if System.Threading.Interlocked.CompareExchange(disposeInvoked, 1, 0) = 0 then
            logDebug "Disposing..." [||]
            
            refreshSub.Dispose()
            checkUnprocessedSub.Dispose()
            shareLoadSub.Dispose()
            proactiveShareLoadSub.Dispose()

            let shardProcessorCount = getShardProcessorCount()
            if shardProcessorCount > 0 then
                logInfo "Stopping all [{0}] shard processor..." [| shardProcessorCount |]

                workingShards 
                |> Seq.choose (fun (KeyValue(shardId, workingShard)) -> 
                    match workingShard with | Started _ -> Some shardId | _ -> None) 
                |> Seq.map stopShardProcessor
                |> Async.Parallel
                |> Async.Ignore
                |> Async.Start
                
                shardProcessorCountChangedEvent.Publish.Where(fun n -> n = 0).Take(1).Wait() |> ignore

                logInfo "Shard processors stopped..." [||]
            
            shardProcessorCountSub.Dispose()

            cts.Cancel()
            cts.Dispose()

            (metricsAgent :> IDisposable).Dispose()

            runningApps.TryRemove(appName) |> ignore

            logDebug "Disposed" [||]

    member internal this.Kinesis          = kinesis
    member internal this.DynamoDB         = dynamoDB
    member internal this.Config           = config
    member internal this.TableName        = tableName
    member internal this.StreamName       = streamName
    member internal this.WorkerId         = workerId    
    member internal this.MetricsAgent     = metricsAgent
    member internal this.ProcessorFactory = processorFactory
    
    member internal this.MarkAsClosed shardId    = markAsClosed shardId |> ignore
    member internal this.StopProcessing shardId  = stopShardProcessor shardId |> ignore

    //#region Factory methods

    static member CreateNew(awsKey    : string, 
                            awsSecret : string, 
                            region    : RegionEndpoint, 
                            appName, streamName, workerId, processorFactory) =
        let config     = new ReactoKinesixConfig()
        let kinesis    = AWSClientFactory.CreateAmazonKinesisClient(awsKey, awsSecret, region)
        let dynamoDB   = AWSClientFactory.CreateAmazonDynamoDBClient(awsKey, awsSecret, region)
        let cloudWatch = AWSClientFactory.CreateAmazonCloudWatchClient(awsKey, awsSecret, region)
        new ReactoKinesixApp(kinesis, dynamoDB, cloudWatch, appName, streamName, workerId, processorFactory, config) :> IReactoKinesixApp

    static member CreateNew(awsKey    : string, 
                            awsSecret : string, 
                            region    : RegionEndpoint, 
                            appName, streamName, workerId, processorFactory, config) =
        let kinesis    = AWSClientFactory.CreateAmazonKinesisClient(awsKey, awsSecret, region)
        let dynamoDB   = AWSClientFactory.CreateAmazonDynamoDBClient(awsKey, awsSecret, region)
        let cloudWatch = AWSClientFactory.CreateAmazonCloudWatchClient(awsKey, awsSecret, region)
        new ReactoKinesixApp(kinesis, dynamoDB, cloudWatch, appName, streamName, workerId, processorFactory, config) :> IReactoKinesixApp

    static member CreateNew(kinesis, dynamoDB, cloudWatch, appName, streamName, workerId, processorFactory) =
        let config = new ReactoKinesixConfig()
        new ReactoKinesixApp(kinesis, dynamoDB, cloudWatch, appName, streamName, workerId, processorFactory, config) :> IReactoKinesixApp

    static member CreateNew(kinesis, dynamoDB, cloudWatch, appName, streamName, workerId, processorFactory, config) =
        new ReactoKinesixApp(kinesis, dynamoDB, cloudWatch, appName, streamName, workerId, processorFactory, config) :> IReactoKinesixApp

    //#endregion Factory methods

    interface IReactoKinesixApp with
        [<CLIEvent>] member this.OnInitialized    = initializedEvent.Publish
        [<CLIEvent>] member this.OnBatchProcessed = batchProcessedEvent.Publish

        member this.StartProcessing (shardId : string) = startShardProcessor (ShardId shardId) |> Async.StartAsPlainTask
        member this.StopProcessing  (shardId : string) = stopShardProcessor  (ShardId shardId) |> Async.StartAsPlainTask
        member this.ChangeProcessorFactory newFactory  = processorFactory <- newFactory

    interface IDisposable with
        member this.Dispose () = 
            GC.SuppressFinalize(this)
            cleanup(true)

    // provide a finalizer so that in the case the consumer forgets to dispose of the app the
    // finalizer will clean up
    override this.Finalize () =
        logger.Warn("Finalizer is invoked. Please ensure that the object is disposed in a deterministic manner instead.")
        cleanup(false)

and internal ReactoKinesixShardProcessor (app : ReactoKinesixApp, shardId : ShardId) as this =
    let loggerName  = sprintf "ReactoKinesixShardProcessor[Stream:%O, Worker:%O, Shard:%O]" app.StreamName app.WorkerId shardId
    let logger      = LogManager.GetLogger(loggerName)
    let logDebug    = logDebug logger
    let logInfo     = logInfo  logger
    let logWarn     = logWarn  logger
    let logError    = logError logger
    
    let disposeInvoked = ref 0
    let cts = new CancellationTokenSource();

    let metricsAgent : MetricsAgent = app.MetricsAgent

    do logInfo "Starting shard processor..." [||]

    //#region Events

    let batchReceivedEvent          = new Event<Iterator * Record[]>()  // when a new batch of records have been received
    let batchErrorEvent             = new Event<Exception>()            // when an error is encountered when processing a record
    let maxRetryExceededEvent       = new Event<Exception>()            // when the max number of retires have been exceeded

    let startProcessingEvent        = new Event<Iterator * Record[] * IRecordProcessor>() // when we start to process a batch of records
    let batchProcessedEvent         = new Event<Iterator * Record[] * IRecordProcessor>() // when a batch has finished processing with the iterator for next batch

    let initializedEvent            = new Event<unit>()           // when the shard processor has been initialized
    let initializationFailedEvent   = new Event<Exception>()      // when an error was caught whist initializing the shard processor
    
    let checkpointEvent             = new Event<SequenceNumber>() // when the latest checkpoint is updated in state table
    let noCheckpointEvent           = new Event<unit>()           // when the latest checkpoint is updated in state table
    let heartbeatEvent              = new Event<unit>()           // when a heartbeat is recorded
    let conditionalCheckFailedEvent = new Event<unit>()           // when a conditional check failure was encountered when writing to the state table

    let shardClosedEvent            = new Event<unit>()           // when the shard is closed and will not return any more data (i.e. NextShardIterator is null)
    let stopProcessingEvent         = new Event<StoppedReason>()  // when the shard processor is trying to stop processing of any further records
    let stoppedEvent                = new Event<StoppedReason>()  // when the shard processor has completed stopped

    let mutable processorFactory    = app.ProcessorFactory
    let newProcessor ()             = match shardId with | ShardId x -> processorFactory.CreateNew x
    let mutable processor           = newProcessor()

    //#endregion

    let updateHeartbeat _ = 
        let work = 
            async {
                logDebug "Sending heartbeat..." [||]

                let! res = DynamoDBUtils.updateHeartbeat 
                                app.DynamoDB 
                                app.TableName 
                                app.WorkerId 
                                shardId 
                           |> Async.Catch
                match res with
                | Choice1Of2 () -> heartbeatEvent.Trigger()
                | Choice2Of2 (Flatten exn) -> 
                    match exn with 
                    | :? ConditionalCheckFailedException -> 
                        conditionalCheckFailedEvent.Trigger()
                    | exn -> 
                        // TODO : what's the right thing to do here?
                        // a) give up, let the next cycle (or next checkpoint update) update the heartbeat
                        // b) retry a few times
                        // c) retry until we succeed
                        // for now, try option a) as it's not entirely critical for one heartbeat update to
                        // succeed, if problem is with DynamoDB and it persists then eventually we'll be
                        // blocked on the checkpoint update too and either succeed eventualy or some other
                        // worker will take over if they were able to successful write to DynamoDB instead
                        // of the current worker
                        logWarn exn "Failed to update heartbeat, ignoring..." [||]
            }
        Async.Start(work, cts.Token)

    let rec updateIsClosed () =
        let work = 
            async {
                let! res = DynamoDBUtils.updateIsClosed 
                                true 
                                app.DynamoDB 
                                app.TableName 
                                app.WorkerId shardId 
                           |> Async.Catch
                match res with
                | Choice1Of2 ()  -> ()
                | Choice2Of2 (Flatten exn) -> 
                    match exn with
                    | :? ConditionalCheckFailedException -> 
                        conditionalCheckFailedEvent.Trigger()
                    | exn -> 
                        logError exn "Failed to set IsClosed flag to true, retrying..." [||]
                        updateIsClosed()
            }
        Async.Start(work, cts.Token)

    let rec updateCheckpoint seqNum = 
        let work = 
            async {
                let! res = DynamoDBUtils.updateCheckpoint 
                                seqNum 
                                app.DynamoDB 
                                app.TableName 
                                app.WorkerId 
                                shardId 
                           |> Async.Catch
                match res with
                | Choice1Of2 () -> 
                    logDebug "Updated sequence number checkpoint [{0}]" [| seqNum |]
                    checkpointEvent.Trigger(seqNum)
                | Choice2Of2 (Flatten exn) -> 
                    match exn with 
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

    let rec fetchNextRecords iterator (seqNum : SequenceNumber option) = 
        async {
            match iterator with
            | EndOfShard -> shardClosedEvent.Trigger()
            | _ -> 
                logDebug "Fetching next records with iterator [{0}]" [| iterator |]

                let! getRecordsResult = 
                    KinesisUtils.getRecords 
                        app.Kinesis 
                        app.Config 
                        app.StreamName 
                        shardId 
                        iterator 
                        app.Config.MaxBatchSize

                match getRecordsResult with
                | Success(nextIterator, batch) when nextIterator = null -> 
                    logDebug "Received batch of [{0}] records, no more records will be available (end of shard)" [| batch.Length |]
                    batchReceivedEvent.Trigger(EndOfShard, batch)
                | Success(nextIterator, batch) -> 
                    logDebug "Received batch of [{0}] records, next iterator [{1}]" [| batch.Length; iterator |]
                    batchReceivedEvent.Trigger(IteratorToken nextIterator, batch)
                | Failure(exn) -> 
                    match exn with
                    | :? ShardCannotBeIteratedException  -> shardClosedEvent.Trigger()
                    | :? ExpiredIteratorException when seqNum.IsSome -> 
                        logInfo """
Iterator [{0}] expired, retrying with Sequence Number [{1}]...
-------------------------------------------------------------------------
WARNING : your processor might be taking too long to process one batch.
Please consider changing the size of the batch via `ReactoKinesixConfig`.
If this problem persists then you run the risk of data loss as Kinesis 
only keep data for 24 hours.
-------------------------------------------------------------------------"""
                                [| iterator; seqNum.Value |]
                        let iterator' = NoIteratorToken <| AfterSequenceNumber seqNum.Value
                        do! fetchNextRecords iterator' seqNum
                    | _ -> do! fetchNextRecords iterator seqNum
        }

    let processBatch (iterator, records : Record[]) =
        if processorFactory <> app.ProcessorFactory then
            logInfo "Processor factory has changed, disposing existing factory..." [||]
            processor.Dispose()
            processor <- newProcessor()

        let recordCount = records.Length
        logDebug "Start processing batch of [{0}] records, next iterator [{1}]" [| recordCount; iterator |]

        let inline tryProcess (records : Record[]) =
            try
                processor.Process(shardId.ToString(), records)
            with 
            | exn -> { Status = Status.Failure exn; Checkpoint = false }

        let inline processWithRetry maxRetries cont = 
            let rec loop retries cont =
                match recordCount with
                | 0 ->
                    batchProcessedEvent.Trigger(iterator, records, processor)
                    noCheckpointEvent.Trigger()
                | n ->
                    match tryProcess records with
                    | { Status = Status.Success; Checkpoint = checkpoint } -> 
                        logDebug "Processed [{0}] records" [| records.Length |]
                        batchProcessedEvent.Trigger(iterator, records, processor)

                        if checkpoint 
                        then updateCheckpoint <| SequenceNumber ((Seq.last records).SequenceNumber)
                        else noCheckpointEvent.Trigger()
                    | { Status = Status.Failure exn } ->
                        batchErrorEvent.Trigger(exn)
                        if retries >= maxRetries 
                        then cont exn 
                             maxRetryExceededEvent.Trigger(exn)
                        else loop (retries + 1) cont

            loop 0 cont

        let maxRetries, maxRetryExceededHandler =
            match app.Config.ErrorHandlingMode with
            | RetryAndSkip n as mode -> 
                n,
                (fun exn -> logWarn exn "Max retry exceeded for [{0}] records. Skipping..." [| records.Length |]
                            processor.OnMaxRetryExceeded(records, mode))
            | RetryAndStop n as mode -> 
                n,
                (fun exn -> logWarn exn "Max retry exceeded for [{0}] records. Stopping..." [| records.Length|]
                            processor.OnMaxRetryExceeded(records, mode)
                            stopProcessingEvent.Trigger(StoppedReason.ErrorInduced))
       
        startProcessingEvent.Trigger(iterator, records, processor)
        processWithRetry maxRetries maxRetryExceededHandler

    let onShardClosed _ =
        logInfo "Shard is closed, no more records will be available." [||]                    
        updateIsClosed()
        app.MarkAsClosed(shardId)
        stoppedEvent.Trigger(StoppedReason.ShardClosed)

    let checkPendingHandoverRequest =
        async {
            let! res = DynamoDBUtils.getHandoverRequest 
                            app.DynamoDB 
                            app.Config 
                            app.TableName 
                            shardId
            match res with            
            | Failure exn  -> 
                logError exn "Failed to check for pending handover requests" [||]
            | Success None -> 
                logDebug "No pending handover request found" [||]
            | Success (Some { ToWorker = toWorker; Expiry = expiry }) -> 
                logDebug "Received handover request to give control of shard to worker [{0}], request expiry at [{1}]" [| toWorker; expiry |]

                // if the request is still valid, then stop processing this shard
                if DateTime.UtcNow <= expiry 
                then stopProcessingEvent.Trigger(StoppedReason.HandedOver)
                else logDebug "Ignoring request as it has already expired" [||]                    
        }

    //#region Event compositions and subscriptions

    // stop processing anymore records if we encountered conditional check
    // errors (indicative of another worker having taken over control of 
    // the shard from us), or if we were explicitly told to stop (indicated 
    // by the stoppingProcessingEvent)
    let conditionalCheckFailed = 
        conditionalCheckFailedEvent
            .Publish
            .Select(fun _ -> StoppedReason.ConditionalCheckFailed)
    let stopProcessing = 
        stopProcessingEvent
            .Publish
            .Merge(conditionalCheckFailed)
            .Take(1)
    let _ = 
        stopProcessing.Subscribe(fun reason -> 
            logInfo "Stopping [{0}]..." [| reason |])

    let stopped = stoppedEvent.Publish.Take(1)
    let _       =
        stopped.Subscribe(fun reason -> 
            logInfo "Processing has stopped [{0}]." [| reason |]
            (this :> IDisposable).Dispose())

    let heartbeatSub = 
        Observable
            .Interval(app.Config.Heartbeat)
            .SkipUntil(initializedEvent.Publish)
            .TakeUntil(stopped)
            .Subscribe(updateHeartbeat)
    
    // a processing cycle start-->processed->checkpoint/no checkpoint
    //                         |              ^
    //                         |              |
    //                         |->error->retry-->retryExceeded
    // is fully complete only when we reach retry exceeded/checkpoint/no checkpoint
    let startProcessing    = startProcessingEvent.Publish
    let processed          = batchProcessedEvent.Publish
    let checkpoint         = checkpointEvent.Publish.Select(fun _ -> ())
    let noCheckpoint       = noCheckpointEvent.Publish
    let maxRetryExceeded   = maxRetryExceededEvent.Publish
    let processingCycleEnd = 
        checkpoint
        |> Observable.merge noCheckpoint
        |> Observable.merge (maxRetryExceeded |> Observable.map ignore)

    let initialized = initializedEvent.Publish

    // signal to process the next batch of records that has been received
    let nextBatch = 
        initialized
            .Merge(processingCycleEnd.Select(fun _ -> ()))
            .TakeUntil(stopProcessing)

    let batchReceived    = batchReceivedEvent.Publish
    let batchReceivedSub = 
        batchReceived.Subscribe(fun (_, records : Record[]) -> 
            metricsAgent.TrackFetched(shardId, records.Length))

    // fetch new records after the previous batch has been processed until 
    // we need to either stop processing or the shard is closed
    let stopFetching = 
        shardClosedEvent.Publish
        |> Observable.map (fun _ -> StoppedReason.ShardClosed)
        |> Observable.merge stopProcessing

    let emptyReceived = 
        batchReceived 
        |> Observable.filter (fun (_, records) -> records.Length = 0)
    let nonEmptyReceived = 
        batchReceived 
        |> Observable.filter (fun (_, records) -> records.Length > 0)

    // start fetching for the next batch of records straight after the last
    // batch was received, with a minor delay if the last batch was empty 
    // to reduce the number of calls to Kinesis during periods of inactivity
    let fetchNext = 
        Observable
            .Delay(emptyReceived, app.Config.EmptyReceiveDelay)
            .Merge nonEmptyReceived
    let fetchNextSub = 
        fetchNext
            .Zip(processed, fun iterator _ -> iterator)
            .TakeUntil(stopFetching)
            .Subscribe(fun (iterator, records) ->
                let seqNum = 
                    match records with
                    | [||] -> None
                    | arr  -> (Seq.last records).SequenceNumber 
                              |> SequenceNumber 
                              |> Some
                Async.StartImmediate(fetchNextRecords iterator seqNum, cts.Token))

    // after we have received the next batch of records, wait for the nextBatch signal.
    // this could include a forced period of delay if the last batch was empty even if this batch is not empty, this is so that
    // we don't spam Kinesis with too many calls when there are no records to process
    let processing = 
        batchReceived
            .Zip(nextBatch, fun receivedBatch _ -> receivedBatch)
            .TakeUntil(stopProcessing)
    let processingSub = processing.Subscribe(processBatch)

    let processedSub = 
        processed
            .Subscribe(fun (_, records : Record[], _) -> 
                let dataSize = records |> Seq.sumBy (fun r -> r.Data.Length)
                metricsAgent.TrackSuccess(shardId, records.Length, dataSize))
    let errorSub = 
        batchErrorEvent
            .Publish
            .Subscribe(fun _ -> metricsAgent.TrackError(shardId))

    // only deal with the shard closed event the first time we see it
    let _ = shardClosedEvent.Publish.Take(1).Subscribe(onShardClosed)

    (* 
     * when the stop is triggered during a current batch then the next checkpoint/no checkpoint 
     * event tells us the current batch is finished so we can trigger the stopped event, e.g.
     *      ---processing---stop----processed----checkpoint
     *      ---processing---stop----processed----no checkpoint
     *      ---processing-------processed--stop--checkpoint
     *      ---processing-------processed--stop--no checkpoint
     * if the stop came between empty receive and processed then it'll be handled by the next case.
     *
     * hence the stop points consist of checkpoint, no checkpoint and received
     *)
    let stopPoints = batchReceived
                     |> Observable.map (fun _ -> ())
                     |> Observable.merge checkpoint
                     |> Observable.merge noCheckpointEvent.Publish

    let stopping = stopProcessing
                   |> Observable.combineLatest stopPoints
                   |> Observable.map fst
                   |> Observable.take 1
    let _ = stopping.Subscribe(fun reason -> stoppedEvent.Trigger reason)
       
    let handoverCheckSub = 
        Observable
            .Interval(app.Config.CheckPendingHandoverRequestFrequency)
            .SkipUntil(initialized)
            .TakeUntil(stopProcessing)
            .Subscribe(fun _ -> 
                Async.Start(checkPendingHandoverRequest, cts.Token))
                
    //#endregion

    //#region Initialization

    // this is the initialization sequence
    let init = 
        let getIterator = function
            | Some seqNum -> NoIteratorToken <| AfterSequenceNumber seqNum
            | _ -> NoIteratorToken TrimHorizon            

        let takeOver fromWorkerId seqNum =
            async {
                // claim ownership of the shard by successfully updating the row
                try
                    do! DynamoDBUtils.updateWorkerId 
                            app.WorkerId 
                            app.DynamoDB 
                            app.TableName 
                            fromWorkerId shardId

                    logDebug "Successfully taken over responsibility for the shard" [||]

                    // the shard has not been processed currently, start from the last checkpoint
                    let iterator = getIterator seqNum
                    Async.Start(fetchNextRecords iterator seqNum, cts.Token)
                    initializedEvent.Trigger()
                with
                | Flatten exn ->
                    match exn with
                    | :? ConditionalCheckFailedException -> 
                        conditionalCheckFailedEvent.Trigger()
                    | exn -> 
                        initializationFailedEvent.Trigger(exn)
            }

        async {
            let! createShardResult = 
                DynamoDBUtils.createShard 
                    app.DynamoDB 
                    app.TableName 
                    app.WorkerId 
                    shardId

            match createShardResult with
            | Failure exn -> initializationFailedEvent.Trigger(exn)
            | Success _   -> 
                let! getShardStatusResult = 
                    DynamoDBUtils.getShardStatus 
                        app.DynamoDB 
                        app.Config 
                        app.TableName 
                        shardId

                match getShardStatusResult with
                | Failure exn -> initializationFailedEvent.Trigger(exn)
                | Success status -> 
                    match status with
                    | NotFound _ -> 
                        logger.Warn("Shard is not found. Please check if it was manually deleted from DynamoDB.")
                        initializationFailedEvent.Trigger(ShardNotFoundException)
                    | Closed _   -> shardClosedEvent.Trigger()
                    | NotProcessing(_, workerId', heartbeat, seqNum) -> 
                        logDebug "Taking over shard which was processed by worker [{0}], last heartbeat [{1}] and checkpoint [{2}]"
                                 [| workerId'; heartbeat; seqNum |]
                        do! takeOver workerId' seqNum

                    | HandingOver(_, fromWorker', toWorker', seqNum) when toWorker' = app.WorkerId ->
                        logDebug "Accepting shard handover from worker [{0}]" [| fromWorker' |]
                        metricsAgent.TrackHandover()

                        do! takeOver fromWorker' seqNum
                    | HandingOver(_, fromWorker', toWorker', _) ->
                        logDebug "Shard is being handed over from worker [{0}] to worker [{1}]" [| fromWorker'; toWorker' |]
                        stoppedEvent.Trigger(StoppedReason.ProcessedByOther)

                    | Processing(_, workerId', seqNum, _) when workerId' <> app.WorkerId ->
                        // the shard is being processed by another worker, for now, give up
                        logDebug "Shard is currently being processed by worker [{0}], last checkpoint [{1}], retiring." [| workerId'; seqNum |]
                        stoppedEvent.Trigger(StoppedReason.ProcessedByOther)                    
                    | Processing(_, workerId', seqNum, Some { FromWorker = fromWorker; ToWorker = toWorker })
                        when workerId' = app.WorkerId && WorkerId fromWorker = app.WorkerId ->

                        logDebug "Cannot resume processing of shard, handover request has been issued by worker [{0}]" [| toWorker |]
                        stoppedEvent.Trigger(StoppedReason.HandedOver)
                    | Processing(_, workerId', seqNum, _) -> 
                        logDebug "Resuming processing of shard, last checkpoint [{0}]" [| seqNum |]

                        // the shard was being processed by this worker, continue from where we left off
                        let iterator = getIterator seqNum
                        Async.Start(fetchNextRecords iterator seqNum, cts.Token)
                        initializedEvent.Trigger()
        }

    // keep retrying failed initializations until it succeeds
    let retryInitSub = 
        initializationFailedEvent.Publish
            .TakeUntil(initialized)
            .Subscribe(fun _ -> Async.Start(init, cts.Token))

    do Async.Start(init, cts.Token)

    //#endregion

    let cleanup (disposing : bool) =
        // ensure that resources are only disposed of once
        if System.Threading.Interlocked.CompareExchange(disposeInvoked, 1, 0) = 0 then
            logDebug "Disposing..." [||]

            cts.Cancel()

            [| heartbeatSub; handoverCheckSub; fetchNextSub; batchReceivedSub; 
               processingSub; processedSub; errorSub;
               retryInitSub; (cts :> IDisposable) |]
            |> Array.iter (fun x -> x.Dispose())

            logDebug "Disposed." [||]

    member this.OnBatchProcessed : IEvent<Iterator * Record[] * IRecordProcessor> = batchProcessedEvent.Publish
    member this.OnStopped : IEvent<StoppedReason> = stoppedEvent.Publish

    member this.Stop () = stopProcessingEvent.Trigger StoppedReason.UserTriggered

    interface IDisposable with
        member this.Dispose () = 
            GC.SuppressFinalize(this)
            cleanup(true)

    // provide a finalizer so that in the case the consumer forgets to dispose of the shard processor the
    // finalizer will clean up
    override this.Finalize () =
        logger.Warn("Finalizer is invoked. Please ensure that the object is disposed in a deterministic manner instead.")
        cleanup(false)