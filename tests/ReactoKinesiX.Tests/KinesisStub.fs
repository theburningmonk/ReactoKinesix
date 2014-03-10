namespace ReactoKinesix.Tests

open System
open System.Collections.Generic
open System.IO
open System.Threading

open Amazon.Kinesis
open Amazon.Kinesis.Model

type KinesisRecord =
    {
        Data            : byte[]
        PartitionKey    : string
        SequenceNumber  : string
    }

    member this.ToDTO () =
        new Record(SequenceNumber = this.SequenceNumber, 
                   PartitionKey   = this.PartitionKey, 
                   Data           = new MemoryStream(this.Data))

type KinesisShard (streamName, limit, shardId : int) =
    let records = new List<string * KinesisRecord>()

    let getSeqNumber (iteratorType : ShardIteratorType) (startingSeqNumber : string) =
        if iteratorType = ShardIteratorType.LATEST then
            fst records.[records.Count - 1]
        elif iteratorType = ShardIteratorType.AFTER_SEQUENCE_NUMBER then
            records 
            |> Seq.skipWhile (fun (seqNum, _) -> seqNum < startingSeqNumber)
            |> Seq.head
            |> fst
        elif iteratorType = ShardIteratorType.AT_SEQUENCE_NUMBER then
            startingSeqNumber
        elif iteratorType = ShardIteratorType.TRIM_HORIZON then
            match records.Count with
            | 0   -> ""
            | lst -> fst records.[0]
        else raise <| TestUtils.UnsafeInit<Amazon.Kinesis.Model.InvalidArgumentException>()

    member this.StreamName  = streamName
    member this.ShardId     = sprintf "shard-%d" shardId

    member this.GetRecords (seqNum, limit) =
        records 
        |> Seq.skipWhile (fun (seqNum', _) -> seqNum' < seqNum)
        |> Seq.tryTake limit
        |> Seq.toArray

    member this.PutRecord (seqNumber, record) = records.Add(seqNumber, record)

    member this.GetIterator (iteratorType : ShardIteratorType, startingSeqNumber : string) = 
        getSeqNumber iteratorType startingSeqNumber |> sprintf "%s-%d-%s" streamName shardId

    member this.ToDTO () =
        let shard = new Shard()
        shard.ShardId             <- this.ShardId
        shard.SequenceNumberRange <- new SequenceNumberRange(StartingSequenceNumber = "", EndingSequenceNumber = "")
        shard.HashKeyRange        <- new HashKeyRange(StartingHashKey = "", EndingHashKey = "")
        shard
    
type KinesisStream (req : CreateStreamRequest, ?limit) =
    let limit  = defaultArg limit 5
    let rand   = new Random(DateTime.UtcNow.Ticks |> int32)
    let shards = Seq.init req.ShardCount (fun n -> new KinesisShard(req.StreamName, limit, n)) |> Seq.toResizeArray
    let seqNum = ref 0

    member this.Name        = req.StreamName
    member this.Shards      = shards
    member this.ShardCount  = shards.Count
    member this.ItemCount   = !seqNum
    member val Status       = StreamStatus.CREATING with get, set

    member this.StreamDescription =
        let desc = new StreamDescription()
        desc.HasMoreShards  <- false
        desc.Shards         <- shards |> Seq.map (fun x -> x.ToDTO()) |> Seq.toResizeArray
        desc.StreamName     <- req.StreamName
        desc.StreamStatus   <- this.Status

        desc

    member this.GetRecords (iterator : string, limit') =
        let limit = match limit' with | 0 -> limit | x -> x
        let [| _; shardId; seqNum |] = iterator.Split('-')
        let shard = shards.[int shardId]      
        shard.GetRecords(seqNum, limit)  

    member this.PutRecord (req : PutRecordRequest) =
        let shard  = shards.[rand.Next(shards.Count)]
        let seqNum = Interlocked.Increment(seqNum) |> sprintf "%010d"
        let record = { Data = req.Data.ToArray(); PartitionKey = req.PartitionKey; SequenceNumber = seqNum }
        
        shard.PutRecord(seqNum, record)
        shard.ShardId, seqNum

    // custom indexer to load a shard by ID
    member this.Item
        with get (shardId : string) =
            match shards |> Seq.tryFind (fun shard -> shard.ShardId = shardId) with
            | Some shard -> shard
            | None -> raise <| TestUtils.UnsafeInit<Amazon.Kinesis.Model.ResourceNotFoundException>()

type KinesisStub () =
    let streams = new Dictionary<string, KinesisStream>()

    let getStream streamName = 
        if not <| streams.ContainsKey streamName then
            raise <| TestUtils.UnsafeInit<Amazon.Kinesis.Model.ResourceNotFoundException>()
        
        streams.[streamName]

    member this.Streams = streams
    
    interface IAmazonKinesis with
        //#region CreateStream

        member this.CreateStream req = 
            if streams.ContainsKey req.StreamName then
                raise <| TestUtils.UnsafeInit<Amazon.Kinesis.Model.ResourceInUseException>()

            streams.Add(req.StreamName, new KinesisStream(req))
            new CreateStreamResponse()

        member this.CreateStreamAsync (req, _) =
            async { return (this :> IAmazonKinesis).CreateStream req } |> Async.StartAsTask

        //#endregion

        //#region DeleteStream

        member this.DeleteStream req =
            let _ = getStream req.StreamName
            streams.Remove req.StreamName |> ignore
            new DeleteStreamResponse()            

        member this.DeleteStreamAsync (req, _) =
            async { return (this :> IAmazonKinesis).DeleteStream req } |> Async.StartAsTask

        //#endregion

        //#region DescribeStream

        member this.DescribeStream req =
            let stream = getStream req.StreamName
            new DescribeStreamResponse(StreamDescription = stream.StreamDescription)

        member this.DescribeStreamAsync (req, _) =
            async { return (this :> IAmazonKinesis).DescribeStream req } |> Async.StartAsTask

        //#endregion

        //#region ListStreams

        member this.ListStreams () =
            new ListStreamsResponse(HasMoreStreams = false, StreamNames = (streams.Keys |> Seq.toResizeArray))

        member this.ListStreams req = (this :> IAmazonKinesis).ListStreams()
        member this.ListStreamsAsync (req, _) =
            async { return (this :> IAmazonKinesis).ListStreams() } |> Async.StartAsTask

        //#endregion

        //#region GetRecords

        member this.GetRecords req =
            let [| streamName; shardId; _ |] = req.ShardIterator.Split('-')
            let stream     = getStream streamName
            let records    = stream.GetRecords(req.ShardIterator, req.Limit)

            let res = new GetRecordsResponse()
            res.Records.AddRange(records |> Seq.map (fun (_, record) -> record.ToDTO()))

            let lastSeqNum =
                match records with
                | [||] -> stream.ItemCount
                | arr  -> arr |> Seq.last |> fst |> int
            res.NextShardIterator <- sprintf "%s-%s-%010d" streamName shardId (lastSeqNum + 1)

            res

        member this.GetRecordsAsync (req, _) =
            async { return (this :> IAmazonKinesis).GetRecords req } |> Async.StartAsTask

        //#endregion

        //#region PutRecord

        member this.PutRecord req =
            let stream = getStream req.StreamName
            let shardId, seqNum = stream.PutRecord req
            new PutRecordResponse(ShardId = shardId, SequenceNumber = seqNum)

        member this.PutRecordAsync (req, _) =
            async { return (this :> IAmazonKinesis).PutRecord req } |> Async.StartAsTask

        //#endregion

        //#region GetShardIterator

        member this.GetShardIterator req =
            let stream   = getStream req.StreamName
            let shard    = stream.[req.ShardId]
            let iterator = shard.GetIterator(req.ShardIteratorType, req.StartingSequenceNumber)

            new GetShardIteratorResponse(ShardIterator = iterator)

        member this.GetShardIteratorAsync (req, _) =
            async { return (this :> IAmazonKinesis).GetShardIterator req } |> Async.StartAsTask

        //#endregion

        member this.MergeShards req                 = raise <| NotImplementedException()
        member this.MergeShardsAsync (req, _)       = raise <| NotImplementedException()

        member this.SplitShard req                  = raise <| NotImplementedException()
        member this.SplitShardAsync (req, _)        = raise <| NotImplementedException()

        member this.Dispose () = ()