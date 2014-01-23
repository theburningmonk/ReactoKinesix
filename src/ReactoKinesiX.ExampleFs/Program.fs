open ReactoKinesix
open ReactoKinesix.Model
open ReactoKinesix.Utils
open log4net
open log4net.Config

[<EntryPoint>]
let main argv = 
    let appName     = "YC-test"
    let streamName  = "YC-test"
    let workerId    = "PHANTOM-fs"

    do BasicConfigurator.Configure() |> ignore
    
    let act (record : Record) =
        let msg = System.Text.Encoding.UTF8.GetString(record.Data)
        printfn "\n\n\n\n\n\n\n\n\n\n%s : %s\n\n\n\n\n\n\n\n\n\n" record.SequenceNumber msg
    
    let maxRetryExceeded (record : Record) (mode : ErrorHandlingMode) =
        printfn "\n\n\n\n\n\n\n\n\n\n%s\n%A\n\n\n\n\n\n\n\n\n\n" record.SequenceNumber mode

    let processor = { new IRecordProcessor with 
                        member this.Process record = act record
                        member this.GetErrorHandlingMode _ = RetryAndStop 3
                        member this.OnMaxRetryExceeded (record, mode) = maxRetryExceeded record mode }
    
    let kinesis    = Amazon.AWSClientFactory.CreateAmazonKinesisClient()
    let dynamoDB   = Amazon.AWSClientFactory.CreateAmazonDynamoDBClient()
    let cloudWatch = Amazon.AWSClientFactory.CreateAmazonCloudWatchClient()

    printfn "Starting client application..."

    let app = ReactoKinesixApp.CreateNew(kinesis, dynamoDB, cloudWatch, appName, streamName, workerId, processor)

    app.OnInitialized.Add(fun _ -> printfn "Client application started")
    app.OnBatchProcessed.Add(fun _ -> printfn "Another batch processed...")

    printfn "Press any key to quit..."
    System.Console.ReadKey() |> ignore

    app.Dispose()

    0 // return an integer exit code
