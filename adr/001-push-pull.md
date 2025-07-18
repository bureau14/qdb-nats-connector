# ADR-001: Pull-Based Message Consumption with Worker Pool

Date: 2025-07-04  
Updated: 2025-07-17 (Simplified to single consumer model)

## Status

Accepted (Updated)

## Context

The NATS connector needs to efficiently consume messages from NATS and write them to QuasarDB. Our primary customer operates multiple processes pulling from NATS streams. Additionally, QuasarDB already provides server-side batching through its async push mechanism, making client-side batching layers an anti-pattern.

Two primary consumption patterns were available:

1. **Push-based**: Using `nats.Subscribe()` with message callbacks
2. **Pull-based**: Using JetStream pull consumers with `Fetch()`/`FetchBatch()`

Within pull-based consumption, we evaluated several architectural approaches:

1. **Coordinator Pattern**: Single process with one consumer distributing work to internal goroutines
2. **Queue Groups**: Multiple processes sharing consumers via JetStream queue groups
3. **Shared Consumers**: Multiple processes share the same consumer for horizontal scaling

## Decision

We chose **pull-based consumption** with a **single shared consumer** model. The connector's scalability and parallelism are achieved through:

1. **Internal worker pool**: Each connector process spawns multiple worker goroutines
2. **Horizontal scaling**: Multiple connector processes can use the same consumer name

The connector uses a single durable pull consumer for the entire stream. The stream configuration determines what subjects are captured - there is no topic filtering at the connector level.

When multiple workers (either within a process or across processes) pull from the same durable consumer, NATS JetStream ensures that each batch of messages is delivered to only one worker, thus distributing the load automatically.

This model provides a simple, robust, and horizontally scalable architecture that is idiomatic to both NATS and modern cloud-native deployments.

## Architectural Details

### Worker Model

Each connector process manages a pool of worker goroutines that pull from a single shared consumer.

```
# Process Instance 1 (4 workers)
--stream EVENTS --consumer qdb-connector --workers 4

# Process Instance 2 (also 4 workers, sharing the same consumer)
--stream EVENTS --consumer qdb-connector --workers 4
```

In this example:
- 8 total workers (4 per process) share the workload from the EVENTS stream
- NATS JetStream distributes messages across all workers automatically
- The stream configuration (not the connector) determines what subjects are processed

### Consumer Management

- A single durable consumer is defined on the JetStream stream
- The connector is configured with a consumer name via `--consumer`
- Multiple workers within a process are configured via `--workers`
- Workers can be added or removed dynamically, with NATS managing the state
- Multiple processes using the same consumer name automatically share the workload

### Scaling

Operators scale the system in two ways:
1. **Vertical scaling**: Increase `--workers` within a process
2. **Horizontal scaling**: Run more connector processes with the same consumer name

This aligns perfectly with container orchestration platforms like Kubernetes, where scaling is achieved by increasing the replica count of the connector deployment.


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

#### Chosen: Shared Consumers
- **Benefit**: True horizontal scaling - multiple instances share workload automatically
- **Benefit**: Natural cloud-based scaling - add containers to increase capacity
- **Benefit**: Flexible deployment - explicit mappings or shared prefix
- **Benefit**: Simple mental model - JetStream handles distribution
- **Benefit**: Aligns with JetStream's design philosophy

### Design Trade-offs

1. **Single Consumer vs Topic Isolation**
   - **Cost**: All messages share the same consumer state
   - **Benefit**: Simpler operations, better load distribution
   - **Mitigation**: Stream configuration provides subject filtering

2. **Configurable Worker Count**
   - **Decision**: Operators can tune worker count per process
   - **Rationale**: Balance between resource usage and throughput
   - **Default**: 1 worker for backward compatibility

3. **Shared Work Queue**
   - **Decision**: All workers pull from the same queue
   - **Rationale**: Better load distribution than static assignment
   - **Trade-off**: Less predictable message ordering

## Consequences

### Positive Consequences

1. **Operational Simplicity**: Single consumer to manage per stream
2. **Horizontal Scalability**: Add/remove workers or processes without coordination
3. **Better Load Distribution**: Workers share the queue evenly
4. **Simple State Management**: One consumer tracks all progress
5. **Clear Monitoring**: Stream-level metrics and lag tracking
6. **Kubernetes Native**: Works with StatefulSets, Deployments, and HPA

### Negative Consequences

1. **Less Granular Control**: Cannot isolate specific subjects to specific workers
2. **Shared Fate**: All messages share the same consumer state
3. **Memory Usage**: Each worker has its own parser and QDB handle

### Operational Implications

1. **Stream Configuration**: Subject filtering happens at the stream level
2. **Scaling**: Increase workers or add instances for more throughput
3. **Monitoring**: Track consumer lag at the stream level
4. **Debugging**: All workers share the same consumer logs

## Update Notes (2025-07-17)

The implementation has been simplified to a single consumer model:

1. **Removed Topic Filtering**: Stream configuration handles subject filtering
2. **Single Consumer**: Replaced per-topic consumers with one shared consumer
3. **Worker Pool**: Added configurable worker count for parallel processing
4. **Simplified Scaling**: Just increase workers or add more processes

This simplification improves operational simplicity while maintaining scalability.

## Future Considerations

1. **Dynamic Worker Scaling**: Could add auto-scaling based on consumer lag

2. **Work Stealing**: Could implement work stealing between workers for better load distribution

3. **Checkpointing**: Currently using JetStream's built-in consumer state. Could add QuasarDB-based checkpointing for reducing dependency.
