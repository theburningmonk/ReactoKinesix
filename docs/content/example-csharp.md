Start by downloading the [Nuget package](https://www.nuget.org/packages/ReactoKinesix/).

```csharp
class Program
{
    static void Main()
    {
        var awsKey = "AKIAJTGWYUCX6G7P2FOQ";
        var awsSecret = "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";
        var region = RegionEndpoint.USEast1;
        var appName = "YC-test";
        var streamName = "YC-test";
        var workerId = "PHANTOM-cs";

        Console.WriteLine("Starting client application...");

        var factory = new MyProcessorFactory();
        var app = 
			ReactoKinesixApp.CreateNew(
				awsKey, awsSecret, region, appName, streamName, workerId, factory);

        app.OnInitialized += (_, evtArgs) => 
			Console.WriteLine("Client application started");
        app.OnBatchProcessed += (_, evtArgs) => 
			Console.WriteLine("Another batch processed...");

        Console.WriteLine("Press any key to quit...");
        Console.ReadKey();

        app.Dispose();
    }
}
```

The above example assumes existence of `MyProcessorFactory`.

```csharp
open ReactoKinesix
open ReactoKinesix.Model

public class MyProcessorFactory : IRecordProcessorFactory
{
    public IRecordProcessor CreateNew(string shardId)
    {
        return new MyProcessor();
    }
}

public class MyProcessor : IRecordProcessor
{
    public ProcessRecordsResult Process(
		string shardId, 
		Record[] records)
    {
        foreach (var record in records)
        {
            var msg = System.Text.Encoding.UTF8.GetString(record.Data);
            Console.WriteLine(
				"You got message [{0}] : {1}", 
				record.SequenceNumber, 
				msg);
        }

        return new ProcessRecordsResult(Status.Success, true);
    }

    public void OnMaxRetryExceeded(
		Record[] records, 
		ErrorHandlingMode errorHandlingMode)
    {
        Console.WriteLine(
			"Batch [count:{0}] failed.", 
			records.Length);
    }

    public void Dispose()
    {
        Console.WriteLine("Processor disposed");
    }
}
```