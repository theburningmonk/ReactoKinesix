(**
As mentioned in the [**Getting Started**](getting-started.html) section of this guide, the `IRecordProcessor` interface requires you to implement these methods:
- `Process`
- `OnMaxRetryExceeded`

Via the configuration, you can choose to retry on error a number of times and then either 
- skip the record
- stop processing this shard altogether 

if the specified retries have been reached and the error still persists then the library will proceed to call the `IRecordProcessor.OnMaxRetryExceeded` method to give you a last chance to handle the *record* before we skip to the next *record* or stop processing the *shard*.

> **Note**: if you specify a retry count of 0 then the *records* will not be retried before skipping/stopping.

> **Note**: you may want to ensure that the data carried by the failing *records* are not lost by implementing a mechanism to fall back to *Amazon SQS* in your implementation of `IRecordProcessor.OnMaxRetryExceeded`. This way you can continue processing the shard without losing data.
> 
> Once captured in *SQS* the data can be processed by another background process and potentially retried for up to 14 days (*SQS*'s max retention period) although in practice if the data cannot be processed with so many attempts you probably want to send out an alert and have an engineer look into it!

#### tl;dr

![Error Flow Chart](http://reacto-kinesix.s3.amazonaws.com/ErrorFlowChart.png)

#### When to use *RetryAndStop*?

> *Amazon Kinesis* uses the *partition key* (which you supply when you push the *record* to *Kinesis*) to calculate a hash which determines which *shard* a *record* will go into. 

When it's absolutely paramount for you to preserve the order in which *records* for a particular *partition key* is processed. For example, all analytic events for a player in a social game will have the same *partition key* and will therefore end up in the same *shard* and if these events must be processed sequentially then you will want to use the *RetryAndStop* error handing mode to ensure that persistent/temporary errors do not cause the events to be processed out-of-order.

> **Note**: if processing of a *shard* is stopped due to the use of the **RetryAndStop** handling mode then the client application will not try to process this *shard* again unless explicitly told to do with when you call the `IReactoKinesixApp.StartProcessing` method. 
> 
> However, other workers/nodes will still take over processing of this *shard*, but if the problem that is causing the *record* to fail is not local to the earlier node, then each and every node that attempts to process the *shard* will also fail and eventually they will all stop trying to process this particular *shard*.

> **Important**: **loss of data is possible** if processing of a *shard* is stopped for a prolonged period and unprocessed *records* become unavailable as *Amazon Kinesis* only retains up to 24 hours worth of data.   

#### When to use *RetryAndSkip*?

In most cases! In order to prevent the build-up of backlogs or in extreme cases the loss of data you should avoid stopping processing of a *shard* in the event of errors except in exceptional circumstances, and instead rely on other mechanisms (such as the use of `Amazon SNS` and `Amazon SQS` as described earlier) to deal with persistent errors.

*)