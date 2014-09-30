namespace ReactoKinesix

open System
open System.Collections.Generic
open System.Globalization
open System.Threading
open System.Timers

open log4net

open Amazon
open Amazon.CloudWatch
open Amazon.CloudWatch.Model
open Amazon.DynamoDBv2
open Amazon.DynamoDBv2.Model
open Amazon.Kinesis
open Amazon.Kinesis.Model

open ReactoKinesix.Model
open ReactoKinesix.Utils

type internal MetricsAgent (cloudWatch : IAmazonCloudWatch,
                            appName    : string,
                            streamName) =
    let logger  = LogManager.GetLogger("MetricsAgent")
    
    let (StreamName streamName) = streamName
    let metricNs, appDimName, streamDimName, shardDimName = "Reacto-KinesiX", "AppName", "StreamName", "ShardId"
    let successMetricName, errorMetricName, sizeMetricName, handoverMetricName, fetchedMetricName = 
        "Process.Success", "Process.Error", "Process.Bytes", "Handover", "Fetched"

    let appDim     = new Dimension(Name = appDimName, Value = appName)
    let streamDim  = new Dimension(Name = streamDimName, Value = streamName)
    let streamDims = [| appDim; streamDim |]

    let getShardDims =
        let getInternal shardId = 
            let shardDim = new Dimension(Name = shardDimName,  Value = shardId)
            [| appDim; streamDim; shardDim |]
        memoize getInternal

    let body (inbox : Agent<MetricsAgentMessage>) =
        // since CloudWatch's lowest granularity is 1 minute, so let's batch up count metrics to the minute interval
        let getPeriodId (timestamp : DateTime) = uint64 <| timestamp.ToString("yyyyMMddHHmm")
        let getMinuteTimestamp (periodId : uint64) = DateTime.ParseExact(string periodId, "yyyyMMddHHmm", CultureInfo.InvariantCulture)

        let metricsData = new Dictionary<uint64 * string, Metric>()

        async {
            while true do
                let! msg = inbox.Receive()
                match msg with
                | IncrMetric(timestamp, dims, unit, metricName, n) ->
                    let periodId = getPeriodId timestamp
                    let key = periodId, metricName
                    match metricsData.TryGetValue key with
                    | true, metric -> metric.AddDatapoint(double n)
                    | _ -> let timestamp = getMinuteTimestamp periodId
                           metricsData.[key] <- Metric.Init(timestamp, dims, unit, metricName, double n)
                | Flush(reply) ->
                    reply.Reply(metricsData.Values |> Seq.toArray)
                    metricsData.Clear()
        }
    let agent = Agent<MetricsAgentMessage>.Start(body)
    do agent.Error.Add(fun exn -> logger.Warn("Metrics agent encountered an unhandled error. Ignoring.", exn))
    
    let pushMetrics = 
        async {
            let! metrics = agent.PostAndAsyncReply Flush
            do! CloudWatchUtils.pushMetrics cloudWatch metricNs metrics
        }
        
    let startTimer onElapsed (interval : TimeSpan)  = 
        let timer = new Timer(interval.TotalMilliseconds)
        do timer.Elapsed.Add onElapsed
        do timer.Start() 
        timer

    let pushTimer  = TimeSpan.FromMinutes(1.0) |> startTimer (fun _ -> Async.Start pushMetrics)

    let disposeInvoked = ref 0
    let cleanup (disposing : bool) =
        if System.Threading.Interlocked.CompareExchange(disposeInvoked, 1, 0) = 0 then
            logger.Debug("Disposing...")
            pushTimer.Dispose()

            try
                Async.RunSynchronously pushMetrics
            with 
            | exn -> logger.Warn("Failed to flush remaining metrics, skipping...", exn)

            logger.Debug("Disposed.")

    member this.TrackHandover () =
        let now = DateTime.UtcNow
        agent.Post <| IncrMetric(now, streamDims, StandardUnit.Count, handoverMetricName, 1)

    member this.TrackFetched (ShardId shardId, count : int) =
        let shardDims, now = getShardDims shardId, DateTime.UtcNow
        agent.Post <| IncrMetric(now, shardDims, StandardUnit.Count, fetchedMetricName, count)

    member this.TrackSuccess (ShardId shardId, count : int, totalPayloadSize : int) = 
        let shardDims, now = getShardDims shardId, DateTime.UtcNow
        agent.Post <| IncrMetric(now, shardDims, StandardUnit.Count, successMetricName, count)
        agent.Post <| IncrMetric(now, shardDims, StandardUnit.Bytes, sizeMetricName, totalPayloadSize)
    
    member this.TrackError (ShardId shardId) = 
        let shardDims, now = getShardDims shardId, DateTime.UtcNow
        agent.Post <| IncrMetric(now, shardDims, StandardUnit.Count, errorMetricName, 1)

    interface IDisposable with
        member this.Dispose () = 
            GC.SuppressFinalize(this)
            cleanup(true)
    
    // provide a finalizer so that in the case the consumer forgets to dispose of the app the
    // finalizer will clean up
    override this.Finalize () =
        logger.Warn("Finalizer is invoked. Please ensure that the object is disposed in a deterministic manner instead.")
        cleanup(false)