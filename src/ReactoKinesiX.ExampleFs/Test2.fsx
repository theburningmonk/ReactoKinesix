// Learn more about F# at http://fsharp.net. See the 'F# Tutorial' project
// for more guidance on F# programming.

#r "bin/Debug/AWSSDK.dll"

open System
open System.IO
open Amazon
open Amazon.Kinesis
open Amazon.Kinesis.Model

let awsKey      = "AWS-KEY"
let awsSecret   = "AWS-SECRET"
let kinesis = Amazon.AWSClientFactory.CreateAmazonKinesisClient(awsKey, awsSecret, RegionEndpoint.USEast1)

let streamName = "YC-test"

let describeReq = DescribeStreamRequest()
describeReq.StreamName <- streamName
let describeRes = kinesis.DescribeStream(describeReq)

let description = describeRes.StreamDescription
let shards = description.Shards

let shard0, shard1, shard2 = shards.[0], shards.[1], shards.[2]


let getRecords () =
    // get iterator
    let req = GetShardIteratorRequest()
    req.StreamName <- streamName
    req.ShardId    <- shard0.ShardId
    req.ShardIteratorType <- ShardIteratorType.TRIM_HORIZON

    let res = kinesis.GetShardIterator(req)
    let iterator = res.ShardIterator

    // get records
    let req' = GetRecordsRequest()
    req'.ShardIterator <- iterator
    let res' = kinesis.GetRecords(req')
    res'.Records


// put records
let putRecord (value : string) =
    let req''   = PutRecordRequest()
    let payload = Text.Encoding.UTF8.GetBytes(value)
    use stream  = new MemoryStream(payload)

    req''.Data  <- stream
    req''.PartitionKey <- Guid.NewGuid().ToString()
    req''.StreamName   <- streamName    
    printfn "Sending record %s" value
    kinesis.PutRecord(req'')

let mergeShards () =
    let req  = MergeShardsRequest()
    req.StreamName <- streamName
    req.ShardToMerge <- shard0.ShardId
    req.AdjacentShardToMerge <- shard1.ShardId

    kinesis.MergeShards(req)

let splitShards () =
    let req = SplitShardRequest()
    req.StreamName <- streamName
    req.NewStartingHashKey <- "170141183460469231731687303715884105728"
    req.ShardToSplit <- shard2.ShardId

    kinesis.SplitShard(req)

mergeShards()
splitShards()

putRecord <| Guid.NewGuid().ToString()

let records = getRecords()

records |> Seq.iter (fun record -> 
    let reader = new StreamReader(record.Data)
    reader.ReadToEnd() |> printfn "%s")
