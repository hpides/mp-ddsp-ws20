# Distributed code-generation streaming engine

This implementation introduces the distribution idea from the distributed hardcoded streaming engine into the code-generation engine. In the following, only the changes made to these two bases are explained. For details about theire concepts and implementation, please refer to documentation of the [distributed_hardcoded_streaming_engine](../distributed_hardcoded_streaming_engine) and the [string_engine](../string_engine)

## Changes made to the concept from [distributed_hardcoded_streaming_engine](../distributed_hardcoded_streaming_engine)

The implementation in the hardcoded distributed engine differs in some point from the implementation here.

In the hardcoded distributed engine the aggregations of both streams where performed sequentially before handing over the result to the join. This is not ideal when looking on performance. Thatswhy both are now executed in individual thread. To avoid that the AllToAll(V) communications of both streams interfere each other, each distribution operator uses an own MPI communcator.

Additionally there is a different logic implemented to close the windows and use their results for joining. First, there is no explicit MPI Barrier anymore, since a kind of synchronization is performed due to the AllToAll call. This leads to the situation, that some nodes might wait for this call to return while they also could have processed more tuples into this slide. For simplicity we assume, that the clock deviation within the same cluster is rather small and that is no significant impact on the window sizes.

## Changes made to the [string_engine](../string_engine)

There are two major changes compared to the non-distributed code generation engine. For explaining them, we need to introduce our concept of distributing data processing first.

### Distribution concept

The most important point here is, that sending data over network is only required if there are windows involved. In plain stream processing each operator only considers individual tuples without any context. 

Surely one could imagine cases, where we want all tuples (of a key) on one node without windowing, e.g. when there is some global value that may be updated by each tuple and the tuple might also be filtered according to that value. For now, we drop the ability to handle this use case, but there is still the possibility to implement this as a additional operator.

This is a consequence from the tests with the distributed hardcoded engine and the conclusion that we currently can not send a major part of all data via network.

So, for now these three things allways go together: Windowing, Aggregating (by key) and Redistributing (by key)

### Merging Aggregation and Windowing operators

This concept of distribution leeds to the first major change compared to the non-distributed engine. It builds on the observation, that windowing usually includes aggregating by a specific key. As before, there are use cases where one wants to window the data without aggregating, e.g. if simply a join is to be performed. But again: This feature is not conceptually disabled and can be implemented later.

For simplicity we agreed on these fixed sequence of steps that is performed, when windowing/aggregation happens in a distributed manner:
1. On the source nodes: Data is inserted into a pre-aggregation map, storing the intermediate result for each key
2. The window closes: All data collected in this slide is repartitioned across the nodes
3. On receiving nodes: Aggregate all intermediate results from this slide to the final intermediate result of this slide and store it
4. Collect all slides relevant for the closed window and combine them for the final result. 

Since each of these steps depend on their previous one and does not add value to a query if performed individually, we decided to combine all these steps in a single operator. Therefore the logic of the Windowing and Aggregation operators of the non-distributed engines where merged. This also simplified implementation a lot, since there is no need to make these operators aware of their context.

### PipelineBraker

As a second change, the distributed code-generation engine introduces the concept of a ''PipelineBraker''. These, allthough allready implicitly present in the non-distributed engine, now explicitly mark those operator that are not able to process data as stream. Currently this applies only for the DistributedWindowAggregation operator.

Defining an operator as ''PipelineBraker'' leads to encapsulation of the entire previous pipeline into a thread
