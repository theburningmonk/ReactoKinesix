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

    /// Delay in trying to pull the stream if the last pull returned no records. Default is 3 second.
    member val EmptyReceiveDelay       = TimeSpan.FromSeconds(3.0) with get, set

    /// Maximum number of retries on DynamoDB operations. Default is 3.
    member val MaxDynamoDBRetries      = 3 with get, set

    /// How frequenty should we check for shard merges/splits in the stream. Default is 1 minute.
    member val CheckStreamChangesFrequency  = TimeSpan.FromMinutes(1.0) with get, set

/// Thrown when the configuration specifies a heartbeat frequence that's greater than the heartbeat timeout
exception InvalidHeartbeatConfiguration of TimeSpan * TimeSpan

/// Thrown when the configruation for MaxDynamoDBRetries is negative
exception NegativeMaxDynamoDBRetriesConfiguration of int

/// Thorwn when initialization of the app failed with the attached inner exception
exception InitializationFailed of Exception

/// Thrown when an app with the same name 
exception AppNameIsAlreadyRunning of string

[<AutoOpen>]
module internal InternalModel =
    type StreamName = 
        | StreamName of string
        override this.ToString () = match this with | StreamName name -> name

    type TableName = 
        | TableName of string
        override this.ToString () = match this with | TableName name -> name

    type ShardId = 
        | ShardId of string
        override this.ToString () = match this with | ShardId id -> id

    type WorkerId = 
        | WorkerId of string
        override this.ToString () = match this with | WorkerId id -> id

    type SequenceNumber = 
        | SequenceNumber of string
        override this.ToString () = match this with | SequenceNumber seqNum -> seqNum

    type IteratorType   = 
        | TrimHorizon                               // starting at the trim horizon (i.e. earliest record available)
        | AtSequenceNumber      of SequenceNumber   // starting at the given sequence number
        | AfterSequenceNumber   of SequenceNumber   // starting immediate after the given sequence number        
        | Latest                                    // starting at the latest record
        override this.ToString () = 
            match this with
            | TrimHorizon                -> "TrimHorizon"
            | AtSequenceNumber seqNum    -> "At (" + seqNum.ToString() + ")"
            | AfterSequenceNumber seqNum -> "After (" + seqNum.ToString() + ")"
            | Latest                     -> "Latest"

    type Iterator       = 
        | IteratorToken         of string           // using the next iterator token from the previous call
        | NoIteratorToken       of IteratorType     // fetch a new iterator token
        override this.ToString () =
            match this with
            | IteratorToken token       -> "IteratorToken(" + token + ")"
            | NoIteratorToken iterType  -> iterType.ToString()
    
    type ShardStatus    = 
        | Removed       // the shard has been removed
        // the shard is new and has not been processed
        | New           of WorkerId * DateTime
        // the shard is there but not currently being processed
        | NotProcessing of WorkerId * DateTime * SequenceNumber
        // the shard is currently being processed by a worker
        | Processing    of WorkerId * SequenceNumber                

    type Result<'Success, 'Failure> =
        | Success   of 'Success
        | Failure   of 'Failure

    type ProcessResult  = Result<SequenceNumber, SequenceNumber * Exception>

    type ControlMessage =
        | StartWorker      of ShardId * AsyncReplyChannel<unit>
        | StopWorker       of ShardId * AsyncReplyChannel<unit>
        | AddKnownShard    of ShardId * AsyncReplyChannel<unit>
        | RemoveKnownShard of ShardId * AsyncReplyChannel<unit>