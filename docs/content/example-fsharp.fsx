(*** hide ***)
#I "../../bin/ReactoKinesix"

(** 
Start by downloading the [Nuget package](https://www.nuget.org/packages/ReactoKinesix/).
*)

#r "AWSSDK.dll"
#r "ReactoKinesix.dll"

open Amazon
open ReactoKinesix
open ReactoKinesix.Model

let awsKey      = "AKIAI5Y767DTOFBUSYAA"
let awsSecret   = "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"
let region      = RegionEndpoint.USEast1
let appName     = "TestApp"
let streamName  = "TestStream"
let workerId    = "LocalHost"

let act (record : Record) =
    let msg = System.Text.Encoding.UTF8.GetString(record.Data)
    printfn "You got message [%s] : %s" record.SequenceNumber msg  

let processBatch (records : Record[]) = 
    records |> Array.iter act
    { Status = Success; Checkpoint = true }

let maxRetryExceeded (records : Record[]) =
    printfn "Batch [count:%d] failed." records.Length
    
let gen () = 
    { new IRecordProcessor with 
        member this.Process(shardId, records) = processBatch records
        member this.OnMaxRetryExceeded(records, _) = maxRetryExceeded records
        member this.Dispose() = printfn "Processor disposed" }

let factory = 
    { new IRecordProcessorFactory with
        member this.CreateNew _ = gen() }

printfn "Starting client application..."
let app = 
    ReactoKinesixApp.CreateNew
        (awsKey, awsSecret, region, appName, streamName, workerId, factory)

app.OnInitialized.Add(fun _ -> printfn "Client application started")
app.OnBatchProcessed.Add(fun _ -> printfn "Another batch processed...")

printfn "Press any key to quit..."
System.Console.ReadKey() |> ignore

app.Dispose()