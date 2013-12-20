namespace ReactoKinesix.Model

open System
open Amazon.DynamoDBv2.DataModel

type ReactoKinesixConfig () = 
    /// Read throughput to read for the DynamoDB table. Default is 10.
    member val DynamoDBReadThroughput  = 10L with get, set

    /// Write throughput to read for the DynamoDB table. Default is 10.
    member val DynamoDBWriteThroughput = 10L with get, set

    /// Suffix used to name your application's state table in DynamoDB. Default is "KinesisState"
    /// If the application name is "MyApp" and suffix is "KinesisState" then the DynamoDB
    /// table for this application will be called MyAppKinesisState.
    member val DynamoDBTableSuffix     = "KinesisState"
    
    /// Heartheat frequency. Default is 30 seconds.
    member val Heartbeat               = TimeSpan.FromSeconds(30.0) with get, set

    /// Timeout for the heartbeat check. Default is 3 minutes.
    member val HeartbeatTimeout        = TimeSpan.FromMinutes(3.0) with get, set

/// Thrown when the configuration specifies a heartbeat frequence that's greater than the heartbeat timeout
exception InvalidHeartbeatConfiguration of TimeSpan * TimeSpan

[<AutoOpen>]
module internal InternalModel =
    type StreamName     = StreamName of string
    type TableName      = TableName  of string
    type ShardId        = ShardId    of string
    type WorkerId       = WorkerId   of string
    type Iterator       = Iterator   of string option
    type SequenceNumber = SequenceNumber of string
    type ShardStatus    = 
        | Removed       // the shard has been removed
        | NotProcessing // the shard is there but not being processed
        | Processing    of WorkerId * DateTime * SequenceNumber