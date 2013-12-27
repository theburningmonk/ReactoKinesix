
#r "bin/Debug/AWSSDK.dll"
#r "bin/Debug/log4net.dll"
#r "bin/Debug/FSharp.Reactive.dll"
#r "bin/Debug/System.Reactive.Core.dll"
#r "bin/Debug/System.Reactive.Interfaces.dll"
#r "bin/Debug/System.Reactive.Linq.dll"
#r "bin/Debug/ReactoKinesix.dll"
//
//#load "Model.fs"
//#load "Utils.fs"
//#load "Client.fs"

open System
open System.IO
open System.Text
open System.Threading
open Amazon
open Amazon.DynamoDBv2.DocumentModel
open Amazon.Kinesis.Model
open ReactoKinesix
open ReactoKinesix.Model
open ReactoKinesix.Utils
open log4net
open log4net.Config

let awsKey      = "AKIAI5Y767DTOFBUSYAA"
let awsSecret   = "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"
let region      = RegionEndpoint.USEast1
let streamName  = "YC-test"

BasicConfigurator.Configure()

let dynamoDB = Amazon.AWSClientFactory.CreateAmazonDynamoDBClient(awsKey, awsSecret, region) 
let kinesis = Amazon.AWSClientFactory.CreateAmazonKinesisClient(awsKey, awsSecret, region) 

let putRecord (payload : string) =
    let req  = new PutRecordRequest(StreamName = streamName, PartitionKey = Guid.NewGuid().ToString())
    req.Data <- new MemoryStream(Encoding.UTF8.GetBytes(payload))
    kinesis.PutRecord(req) |> ignore

let act (record : Record) =
    let msg = Encoding.UTF8.GetString(record.Data)
    printfn "\n\n\n\n\n\n\n\n\n\n%s : %s\n\n\n\n\n\n\n\n\n\n" record.SequenceNumber msg
    
    failwith "oops"

let act2 (record : Record) =
    let msg = Encoding.UTF8.GetString(record.Data)
    printfn """

=================================================
=================================================
=================================================
%s : %s
=================================================
=================================================
=================================================

"""         record.SequenceNumber <| msg
    

let act3 (record : Record) =
    printfn "\n\n\n\n\n\n\n\n\n\n\n\nSleeping...\n\n\n\n\n\n\n\n\n\n\n"
    Thread.Sleep(5000)

let maxRetryExceeded (record : Record) (mode : ErrorHandlingMode) =
    printfn "\n\n\n\n\n\n\n\n\n\n%s\n%A\n\n\n\n\n\n\n\n\n\n" record.SequenceNumber mode

let processor = { new IRecordProcessor with 
                    member this.Process record = act record
                    member this.GetErrorHandlingMode _ = RetryAndStop 3
                    member this.OnMaxRetryExceeded (record, mode) = maxRetryExceeded record mode }
let processor2 = { new IRecordProcessor with 
                    member this.Process record = act2 record 
                    member this.GetErrorHandlingMode _ = RetryAndStop 3
                    member this.OnMaxRetryExceeded (record, mode) = maxRetryExceeded record mode }
//let processor3 = { new IRecordProcessor with member this.Process record = act3 record }

let app = ReactoKinesixApp.CreateNew(awsKey, awsSecret,region, "YC-test", streamName, "PHANTOM", processor)

app.StartProcessing("shardId-000000000003")
app.StartProcessing("shardId-000000000004")
app.StopProcessing("shardId-000000000003")
app.StopProcessing("shardId-000000000004")
//app.ChangeProcessor(processor3)

//{ 1..100 } |> Seq.iter (fun i -> putRecord <| i.ToString())

(app :> IDisposable).Dispose()


let app2 = ReactoKinesixApp.CreateNew(awsKey, awsSecret,region, "YC-test", streamName, "PHANTOM-2", processor2)
