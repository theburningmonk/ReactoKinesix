using System;
using System.Text;
using Amazon;
using ReactoKinesix.Model;

using log4net.Config;

namespace ReactoKinesix.ExampleCs
{
    public class MyProcessor : IRecordProcessor
    {
        public ProcessRecordsResult Process(Record[] records)
        {
            foreach (var record in records)
            {
                var msg = Encoding.UTF8.GetString(record.Data);
                Console.WriteLine("\n\n\n\n\n\n\n\n\n\n{0} : {1}\n\n\n\n\n\n\n\n\n\n", record.SequenceNumber, msg);
            }

            return new ProcessRecordsResult(Status.Success, true);
        }

        public void OnMaxRetryExceeded(Record[] records, ErrorHandlingMode errorHandlingMode)
        {
            Console.WriteLine("\n\n\n\n\n\n\n\n\n\n{0}\n{1}\n\n\n\n\n\n\n\n\n\n", records.Length, errorHandlingMode);
        }

        public void Dispose()
        {
            Console.WriteLine("Processor disposed");
        }
    }

    public class MyProcessorFactory : IRecordProcessorFactory
    {
        public IRecordProcessor CreateNew(string obj0)
        {
            return new MyProcessor();
        }
    }

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

            BasicConfigurator.Configure();

            var processor = new MyProcessor();

            Console.WriteLine("Starting client application...");

            var factory = new MyProcessorFactory();
            var app = ReactoKinesixApp.CreateNew(awsKey, awsSecret, region, appName, streamName, workerId, factory);

            app.OnInitialized += (_, evtArgs) => Console.WriteLine("Client application started");
            app.OnBatchProcessed += (_, evtArgs) => Console.WriteLine("Another batch processed...");

            Console.WriteLine("Press any key to quit...");
            Console.ReadKey();

            app.Dispose();
        }
    }
}
