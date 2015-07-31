(**
As you scale up a *stream* by adding more *shards* to it, you will need to increase your processing capabilities too, and within *Amazon EC2* you will be able to do that by setting up *Auto Scaling Groups* to scale up your cluster of *EC2* instances based on CPU or Network in/out depending on whether your instances become CPU or network bound when under load.  

Alternatively, you can also **scale up** your deployment by using bigger, more powerful *EC2* instance types, though generally speaking you'll eventually need to scale out at some point as your application grows and requires more and more throughput, so it's advantageous to take the distributed aspect of your *Kinesis*-consuming application into consideration at the earliest opportunity. 

> **Important**: as of now, scaling down nodes running the client application will require graceful handling (i.e. you need to **dispose** of the running `IReactoKinesixApp` instance and wait for its `Dispose` method to complete) to ensure you don't lose any progress when processing a batch of *records* and that when another node takes over processing of the *shards* it wouldn't end up processing some of the same *records* again.

#### Distributing the processing of shards

This library distributes and balances the load across a cluster of workers via a simple mechanism whereby workers who are processing fewer number of *shards* will request workers who are processing **at least 2 more** *shards* to hand over one of the *shards* they're currently processing.

Since the workers form a **master-less** network, this process happens independently on each of the workers when:
- the worker has become idle, i.e. not processing any *shards*
- the configured **LoadBalanceFrequency** has passed (see [Configuring client application](features.html#Configuring-the-client-application))

To keep the decision making process simple, only one worker should be making handover requests at a time and only when all the *shards* are actively being processed, though this limitation might be removed in future versions.

Using this approach, when multiple workers are started up at the same time it'll take several iterations to achieve a balanced distribution of load across the workers. Let's illustrate how this process works by walking through two examples:

**Example 1 - a new worker joins a cluster of two workers**

![Example 1](http://reacto-kinesix.s3.amazonaws.com/HandoverRequestFlowChart-1.png)

As you can see, in this simple case it took two iterations to balance the cluster. In the second iteration *Worker 3* issued a handover request only to *Worker 1* because only workers who are processing at least 2 more shards is issued a request.

**Example 2 - two new workers join a cluster of two workers**

![Example 2](http://reacto-kinesix.s3.amazonaws.com/HandoverRequestFlowChart-2.png)


In this example depending on the timing of events, there are a number of ways in which things can play out but in the end you should have a cluster of 4 workers with one processing 3 shards and the rest 2 shards each. 

In a parallel universe with a different turn of events, things might have turned out slightly differently:

![Example 2-Alt](http://reacto-kinesix.s3.amazonaws.com/HandoverRequestFlowChart-2-ver2.png)

As you can see, it took the same number of iterations to achieve essentially the same result!

> **Remarks**: whilst it might have been easier to implement the distribution of *shards* using a topology with a master node (ala [Zookeeper](http://zookeeper.apache.org/)), or a distributed consensus algorithm such as [Raft](http://raftconsensus.github.io/) or [Paxos](http://en.wikipedia.org/wiki/Paxos_(computer_science)), both require making assumptions about connectivity between workers which I did not think is justified at the library level.
> 
> For instance, it's reasonable to assume a configuration whereby workers are distributed across on-premise and cloud-hosted resources where both can access *Amazon*'s services but connectivity between the clusters is not guaranteed (you might not want to open up public access to your *EC2* instances).

#### Recovering from loss of workers

Each of the workers routinely (based on the configured **CheckUnprocessedShardsFrequency**, see [Configuring client application](features.html#Configuring-the-client-application)) checks to see if there are any *shards* that are not being processed based on the timestamp of the last heartbeat and the first worker that successfully updates the *DynamoDB* entry for the *shard* with its *worker ID* will resume responsibility of processing this *shard*. 

So if a worker is terminated, the remaining workers will see this once the configured heartbeat time out has expired and one of them will succeed in taking over the processing of this *shard*.

*)