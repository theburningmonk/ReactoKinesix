namespace ReactoKinesix.Model

open System
open Amazon.DynamoDBv2.DataModel

type KinesisState = 
    {
        /// Unique shard ID
        ShardId         : string

        /// Timestamp of the last heartbeat.
        LastHeartbeat   : DateTime

        /// ID that identifies the worker currently processing this shard.
        WorkerId        : string
    }

type ReactoKinesixConfig () = 
    /// Default read throughput to read for the DynamoDB table.
    member val DynamoDBReadThroughput  = 10L with get, set

    /// Default read throughput to read for the DynamoDB table.
    member val DynamoDBWriteThroughput = 10L with get, set

    /// Default suffix used to name your application's DynamoDB table.
    /// If the application name is "MyApp" and suffix is "KinesisState" then the DynamoDB
    /// table for this application will be called MyAppKinesisState
    member val DynamoDBTableSuffix     = "KinesisState"