(*** hide ***)
#I "../../bin/ReactoKinesix"

(**
### Before you start

Please familiarize yourself with how **Amazon Kinesis** works by looking through its online [documentations](http://aws.amazon.com/documentation/kinesis/), in particular its [Key Concepts](http://docs.aws.amazon.com/kinesis/latest/dev/key-concepts.html) and [Limitations](http://docs.aws.amazon.com/kinesis/latest/dev/service-sizes-and-limits.html).


### Getting Started

Download and install the library from Nuget [here](https://www.nuget.org/packages/ReactoKinesix/).

<a href="https://www.nuget.org/packages/ReactoKinesix/"><img src="https://raw.github.com/theburningmonk/ReactoKinesiX/develop/nuget/banner.png"/></a>

This library enables you to create a client application which consumes and processes records that have been pushed to an *Amazon Kinesis* **stream** by taking care of most of the plumbing involved.

To process incoming records, you need to provide an implementation for the `IRecordProcessor` interface which has the following methods:

<table>
	<tbody>
		<tr>
			<td><strong>Process</strong></td>
			<td><p>Process a batch of <i>records</i> received from the <i>Kinesis Stream</i>. This method returns an instance of <i>ProcessRecordsResult</i> where you can indicate:</p>
				<ul>
					<li>whether the processing was successful, if an unhandled exception is thrown from this method then this defaults to <i>Status.Failure</i>, and</li>
					<li>if a checkpoint should be placed on the shard immediatel after the last record in the batch. If the current worker terminates prematurely (due to hardware failure, for instance) then another worker can resume processing of the shard from the last checkpoint</li>
				</ul>
			</td>
		</tr>		
		<tr>
			<td><strong>OnMaxRetryExceeded</strong></td>
			<td><p>Last chance to deal with a failing batch of <i>records</i> when the number of retry attempts specified in the configuration has been reached.</p>
				<p>For example, you might choose to:</p>
				<ul>
					<li>push the data in the <i>records</i> to <i>Amazon SQS</i> for processing later</li>
					<li>send out notification via <i>Amazon SNS</i>
					<li>...
				</ul>
			</td>
		</tr>
	</tbody>
</table> 

To start, you can create a client application by calling the static method `ReactoKinesixApp.CreateNew` which returns a running instance of `IReactoKinesixApp` that will start processing *records* from the *stream* straight away.

You will notice that the `ReactoKinesixApp.CreateNew` requires an instance of `IRecordProcessorFactory` rather than an instance of `IRecordProcessor`. The rationale for this decision is that it enables 

#### Tracking the state of your client application

To enable us to track the state of your client application (e.g. what *shards* are we processing and where in the stream of *records* did we get to (a checkpoint so that we can easily return to where we stopped at at a later time), the client application uses a *Amazon DynamoDB* table (such as the following) to store the necessary state information for each client application.
![Example state table](http://reacto-kinesix.s3.amazonaws.com/reactokinesix-state-table.png)

#### Assigning Worker IDs

Each node (e.g. *EC2* instance running the client application) that is processing records from a *stream* should be given a unique *worker ID* to identify itself. If you're running your client application within *Amazon EC2*, then *Instance ID* is a perfect choice to act as a meaningful *worker ID*.

#### F# Example
*)

#r "AWSSDK.dll"
#r "ReactoKinesix.dll"

open Amazon
open ReactoKinesix
open ReactoKinesix.Model

let awsKey      = "AKIAI5Y767DTOFBUSYAA"
let awsSecret   = "zollLGekGcjIdFvCzvtbyf9OfCI1R3nvjtkSQgSM"
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