# ADR-001: Pull-Based Message Consumption with Independent Workers

Date: 2025-07-04

## Status

Accepted

## Context

The NATS connector needs to efficiently consume messages from NATS and write them to QuasarDB. Our primary customer operates multiple processes pulling from different NATS topics and requires flexible topic distribution across connector instances. Additionally, QuasarDB already provides server-side batching through its async push mechanism, making client-side batching layers an anti-pattern.

Two primary consumption patterns were available:

1. **Push-based**: Using `nats.Subscribe()` with message callbacks
2. **Pull-based**: Using JetStream pull consumers with `Fetch()`/`FetchBatch()`

Within pull-based consumption, we evaluated several architectural approaches:

1. **Coordinator Pattern**: Single process with one consumer distributing work to internal goroutines
2. **Queue Groups**: Multiple processes sharing consumers via JetStream queue groups
3. **Independent Workers**: Each process manages independent consumers for its assigned topics

## Decision

We chose **pull-based consumption**. The connector's scalability and parallelism are achieved by running multiple instances of the connector process. NATS JetStream handles the load balancing of messages between these instances.

Each connector process can be configured to handle one or more topic subscriptions. For each subscription, the connector creates a **durable pull consumer**. The name of this durable consumer should be shared across all connector instances that are intended to share the workload for that subscription.

When multiple connector instances (workers) pull from the same durable consumer name, NATS JetStream ensures that each batch of messages is delivered to only one worker, thus distributing the load automatically.

This model provides a simple, robust, and horizontally scalable architecture that is idiomatic to both NATS and modern cloud-native deployments.

## Architectural Details

### Worker Model

Each connector process is a "worker". A single process can be configured to handle multiple topic subscriptions.

```
# Process Instance 1 (handles two topics)
--topic "sensors.temp.>" --durable-name "temp-processor"
--topic "sensors.pressure.>" --durable-name "pressure-processor"

# Process Instance 2 (also handles temp data, sharing the load)
--topic "sensors.temp.>" --durable-name "temp-processor"
```

In this example:
- The load for `sensors.temp.>` is shared between Process 1 and Process 2 because they use the same durable consumer name (`temp-processor`).
- The load for `sensors.pressure.>` is handled exclusively by Process 1.

### Consumer Management

- Consumers are durable and defined on the JetStream server.
- The connector instances are configured with the durable name and stream name to attach to.
- This allows workers to be added or removed dynamically, with NATS managing the state.
- The `--durable-name` flag provides explicit control over which workers share which queues.

### Topic Distribution & Scaling

Operators scale the system by running more connector processes.
- **To add capacity for a topic**: Start a new connector instance pointing to the same stream and using the same durable consumer name.
- **To partition work**: Use different durable consumer names for different data types or geographic regions.
- This aligns perfectly with container orchestration platforms like Kubernetes, where scaling is achieved by increasing the replica count of the connector deployment.


## Rationale

### Why Pull-Based Over Push-Based

1. **Batch Efficiency**: Parsing overhead is the primary CPU bottleneck. Batch processing amortizes this cost across multiple messages.

2. **Backpressure Control**: Pull consumers fetch only what they can process, preventing memory buildup during QuasarDB slowdowns.

3. **Operational Alignment**: Matches patterns that operators already understand.

### Why Independent Workers Over Alternatives

#### Rejected: Coordinator Pattern
- **Problem**: Single process limitation contradicts Kubernetes horizontal scaling requirements
- **Problem**: Single point of failure for all topic processing
- **Problem**: Cannot perform rolling updates without downtime

#### Rejected: Queue Groups
- **Problem**: JetStream queue groups require explicit `DeliverGroup` configuration
- **Problem**: More complex than independent consumers
- **Problem**: Message ordering becomes unpredictable across workers

#### Chosen: Independent Workers
- **Benefit**: Natural cloud-based scaling - add containers with different topic subsets
- **Benefit**: Complete fault isolation - one topic's issues don't affect others
- **Benefit**: Simple mental model - each topic has its own processing pipeline
- **Benefit**: Flexible deployment - same binary handles any topic combination
- **Benefit**: Matches existing Kinesis connector architecture

### Design Trade-offs

1. **Multiple Consumers vs Resource Usage**
   - **Cost**: Each consumer maintains separate ACK state in JetStream
   - **Benefit**: Independent progress tracking per topic type
   - **Mitigation**: Consumers are lightweight; even 100 consumers have minimal overhead

2. **Fixed Worker Count vs Dynamic Scaling**
   - **Decision**: One goroutine per topic (no configurable worker count)
   - **Rationale**: Simplicity over configuration complexity
   - **Note**: Goroutines are lightweight; 20 goroutines for 20 topics is fine

3. **Sequential Batch Processing**
   - **Decision**: No parallelism within batch parsing
   - **Rationale**: CPU cache efficiency outweighs parallel overhead for typical batch sizes
   - **Future**: Can add intra-batch parallelism if profiling shows need

## Consequences

### Positive Consequences

1. **Operational Flexibility**: Any process can handle any topics via simple CLI flags
2. **Horizontal Scalability**: Add/remove processes without coordination
3. **Fault Isolation**: Topic failures don't cascade
4. **Simple State Management**: Each consumer tracks its own progress
5. **Clear Monitoring**: Per-topic metrics and lag tracking
6. **Kubernetes Native**: Works with StatefulSets, Deployments, and HPA

### Negative Consequences

1. **No Automatic Load Balancing**: Operators must manually distribute topics
2. **Consumer Proliferation**: Many topics mean many consumers to monitor
3. **No Shared State**: Cannot easily move topics between processes without replay
4. **Memory Multiplication**: Each worker has its own parser and QDB handle

### Operational Implications

1. **Topic Assignment**: Operators explicitly control which instance handles which topics
2. **Scaling**: Add instances with non-overlapping topic filters
3. **Monitoring**: Track consumer lag per topic, not per process
4. **Debugging**: Each topic's processing is independent - check specific consumer logs

## Future Considerations

1. **Single Consumer Mode**: Source interface abstraction allows future addition of single-consumer-multiple-topics mode if needed

2. **Dynamic Rebalancing**: Could add coordinator service for automatic topic distribution (separate from connectors)

3. **Checkpointing**: Currently using JetStream's built-in consumer state. Could add QuasarDB-based checkpointing for reducing dependency.
