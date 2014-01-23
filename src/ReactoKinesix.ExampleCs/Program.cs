using System;
using System.Text;

using ReactoKinesix.Model;

using log4net.Config;

namespace ReactoKinesix.ExampleCs
{
    class MyProcessor : IRecordProcessor
    {
        public void Process(Record record)
        {
            var msg = Encoding.UTF8.GetString(record.Data);
            Console.WriteLine("\n\n\n\n\n\n\n\n\n\n{0} : {1}\n\n\n\n\n\n\n\n\n\n", record.SequenceNumber, msg);
        }

        public ErrorHandlingMode GetErrorHandlingMode(Record record)
        {
            return ErrorHandlingMode.NewRetryAndStop(3);
        }

        public void OnMaxRetryExceeded(Record record, ErrorHandlingMode errorHandlingMode)
        {
            Console.WriteLine("\n\n\n\n\n\n\n\n\n\n{0}\n{1}\n\n\n\n\n\n\n\n\n\n", record.SequenceNumber, errorHandlingMode);
        }
    }

    class Program
    {
        static void Main()
        {
            var appName = "YC-test";
            var streamName = "YC-test";
            var workerId = "PHANTOM-cs";

            BasicConfigurator.Configure();

            var processor = new MyProcessor();

            var kinesis = Amazon.AWSClientFactory.CreateAmazonKinesisClient();
            var dynamoDb = Amazon.AWSClientFactory.CreateAmazonDynamoDBClient();
            var cloudWatch = Amazon.AWSClientFactory.CreateAmazonCloudWatchClient();

            Console.WriteLine("Starting client application...");

            var app = ReactoKinesixApp.CreateNew(kinesis, dynamoDb, cloudWatch, appName, streamName, workerId, processor);

            app.OnInitialized += (_, evtArgs) => Console.WriteLine("Client application started");
            app.OnBatchProcessed += (_, evtArgs) => Console.WriteLine("Another batch processed...");

            Console.WriteLine("Press any key to quit...");
            Console.ReadKey();

            app.Dispose();
        }
    }
}
