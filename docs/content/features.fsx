(**
### Stopping and Starting processing of a shard

If for some reason you need to stop processing a *shard*, and restart it later, you can call the `IReactoKinesixApp.StopProcessing` and `IReactoKinesixApp.StartProcessing` methods with a *shard ID*.

> **Note**: when stopping processing of a *shard*, in order to avoid lost of progress and potentially process the same *records* more than once when processing is resumed, processing of the *shard* will come to a stop only after we have managed to finish processing the current batch of *records* that have been received and that the checkpoint has been updated successfully in *Amazon DynamoDB*.  

> **Note**: if you are running the client application on multiple nodes then you'll need to call the stop/start processing method on all the nodes otherwise another node will simply take over processing of the *shard* when the heartbeat time out has lapsed.

### Changing processor on the fly

You can also change the `IRecordProcessorFactory` implementation used by the client application at runtime, by calling the `IReactoKinesixApp.ChangeProcessorFactory` method.

> **Note**: if you are running the client application on multiple nodes then you'll need to call the `IReactoKinesixApp.ChangeProcessorFactory` method on all the nodes.

### Stopping the client application

To completely stop the client application and release all the resources currently used, simply **dispose** of the running `IReactoKinesixApp` instance. Doing so will stop the processing of all the shards whilst still making sure that the application is kept in a consistent state so that we are able to resume later from where we left off without risk processing the same *records* again.


### Handling shard merge/split

When you [merge](http://docs.aws.amazon.com/kinesis/latest/APIReference/API_MergeShards.html) or [split](http://docs.aws.amazon.com/kinesis/latest/APIReference/API_SplitShard.html) *shards* in *Amazon Kinesis*, it will create new *shard(s)* and the old *shards* will be closed (but still available to pull records from for 1 day). When this happens the client application will see the new *shards* and start processing them as soon as they become available, and the old *shards* will be processed until all their records have been processed. **This is handled automatically** by this library.

### Configuring the client application

Whilst you don't need to specify a configuration when creating a new client application using the `ReactoKinesixApp.CreateNew` static method, a **default configuration is used** with the following settings:

<table>
	<thead>
		<tr>
			<td><strong>Configuration</strong></td>
			<td><strong>Default Value</strong></td>
			<td><strong>Description</strong></td>
		</tr>
	</thead>
	<tbody>
		<tr>
			<td>DynamoDBReadThroughput</td>
			<td>10</td>
			<td>Read throughput to use for the DynamoDB table.</td>
		</tr>
		<tr>
			<td>DynamoDBWriteThroughput</td>
			<td>10</td>
			<td>Write throughput to use for the DynamoDB table.</td>
		</tr>
		<tr>
			<td>DynamoDBTableSuffix</td>
			<td>KinesisState</td>
			<td>Suffix used to name your application's state table in DynamoDB.</td>
		</tr>
		<tr>
			<td>Heartbeat</td>
			<td>30 seconds</td>
			<td>Heartheat frequency.</td>
		</tr>
		<tr>
			<td>HeartbeatTimeout</td>
			<td>3 minutes</td>
			<td>Timeout for the heartbeat check.</td>
		</tr>
		<tr>
			<td>EmptyReceiveDelay</td>
			<td>3 seconds</td>
			<td>Delay in trying to pull the stream if the last pull returned no records.</td>
		</tr>
		<tr>
			<td>MaxDynamoDBRetries</td>
			<td>3</td>
			<td>Maximum number of retries on DynamoDB operations.</td>
		</tr>
		<tr>
			<td>MaxKinesisRetries</td>
			<td>3</td>
			<td>Maximum number of retries on Kinesis operations.</td>
		</tr>
		<tr>
			<td>CheckStreamChangesFrequency</td>
			<td>1 minute</td>
			<td>How frequently should we check for shard merges/splits in the stream.</td>
		</tr>
		<tr>
			<td>CheckUnprocessedShardsFrequency</td>
			<td>1 minute</td>
			<td>How frequently should we check for shards whose worker has died.</td>
		</tr>
		<tr>
			<td>LoadBalanceFrequency</td>
			<td>3 minutes</td>
			<td>How frequently should we try to balance the load amongst the workers.</td>
		</tr>
		<tr>
			<td>HandoverRequestExpiry</td>
			<td>10 minutes</td>
			<td>How much time to allow a handover request to complete.</td>
		</tr>
		<tr>
			<td>CheckPendingHandoverRequestFrequency</td>
			<td>1 minute</td>
			<td>How frequently should we check for pending handover requests for a shard.</td>
		</tr>
		<tr>
			<td>ErrorHandlingMode</td>
			<td>Retry twice and then skip</td>
			<td>How to handle errors.</td>
		</tr>
		<tr>
			<td>MaxBatchSize</td>
			<td>10,000</td>
			<td>Max number of records per batch.</td>
		</tr>
	</tbody>
</table> 
--------
If you need to use a different configuration to the default, then simply create an instance of `ReactoKinesixConfig` with the configurations you want and pass it into the `ReactoKinesixApp.CreateNew` when creating your client application.

> **Important**: if you know that your application will use a **large number** of *shards* and worker nodes then you will want to **increase the read and write throughput** for the *DynamoDB* table otherwise database operations are likely to be throttled on a regular basis and **cause delays in processing your records**.

*)