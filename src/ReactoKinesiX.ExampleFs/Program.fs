open Amazon
open ReactoKinesix
open ReactoKinesix.Model
open ReactoKinesix.Utils
open log4net
open log4net.Config

[<EntryPoint>]
let main argv = 
    let awsKey      = "AKIAJTGWYUCX6G7P2FOQ"
    let awsSecret   = "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"
    let region      = RegionEndpoint.USEast1
    let appName     = "YC-test"
    let streamName  = "YC-test"
    let workerId    = "PHANTOM-fs"

    do BasicConfigurator.Configure() |> ignore
    
    let act (records : Record[]) =
        records |> Array.iter (fun record ->
            let msg = System.Text.Encoding.UTF8.GetString(record.Data)
            printfn "\n\n\n\n\n\n\n\n\n\n%s : %s\n\n\n\n\n\n\n\n\n\n" record.SequenceNumber msg)
        { Status = Success; Checkpoint = true }
    
    let maxRetryExceeded (records : Record[]) (mode : ErrorHandlingMode) =
        printfn "\n\n\n\n\n\n\n\n\n\n%d Records\n%A\n\n\n\n\n\n\n\n\n\n" records.Length mode

    let gen () = { new IRecordProcessor with 
                        member this.Process records = act records
                        member this.OnMaxRetryExceeded (records, mode) = maxRetryExceeded records mode
                        member this.Dispose() = printfn "Processor disposed" }

    let factory = { new IRecordProcessorFactory with
                        member this.CreateNew _ = gen() }

    printfn "Starting client application..."

    let app = ReactoKinesixApp.CreateNew(awsKey, awsSecret, region, appName, streamName, workerId, factory)

    app.OnInitialized.Add(fun _ -> printfn "Client application started")
    app.OnBatchProcessed.Add(fun _ -> printfn "Another batch processed...")

    printfn "Press any key to quit..."
    System.Console.ReadKey() |> ignore

    app.Dispose()

    0 // return an integer exit code
