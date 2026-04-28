# ADR-002: Data Flow Architecture

## Status
Accepted

## Context
The qdb-nats-connector needs to reliably transfer messages from NATS JetStream to QuasarDB while handling various failure scenarios. The system must:
- Process hundreds of millions of messages per day
- Handle parse failures gracefully
- Prevent data loss during transient failures
- Maintain high throughput under load
- Provide clear operational semantics

## Decision
We implement a simplified, synchronous batch processing pipeline within each worker instance. This model is designed for simplicity, stability, and horizontal scalability.

### 1. Batch Processing Flow

The data flow within a single worker instance is a direct, synchronous pipeline.

```
┌─────────────┐     ┌──────────────┐     ┌─────────────┐     ┌────────────┐
│    NATS     │────▶│    Worker    │────▶│   Parser    │────▶│  QuasarDB  │
│  JetStream  │     │(fetches batch)│    │(parses batch)│    │(writes batch)│
└─────────────┘     └──────────────┘     └─────────────┘     └────────────┘
       │                                                            │
       │◀────────────────────────────────────────────────────────────┘
                                ACK/NACK Batch
```

### 2. Processing Steps

The worker follows a simple, linear process for each batch:

```
1. FETCH BATCH
   └─▶ Get a batch of messages from a NATS JetStream pull consumer.
   
2. PARSE BATCH
   ├─▶ Iterate through each message in the batch.
   ├─▶ Parse valid messages and add them to a list for writing.
   └─▶ Immediately NACK messages that fail to parse and remove them from the list.
   
3. CHECK FOR WRITABLE DATA
   └─▶ If all messages in the batch failed parsing, the loop finishes.
   
4. WRITE BATCH TO QUASARDB (with circuit breaker)
   ├─▶ Write the entire list of valid, parsed messages to QuasarDB in a single transaction.
   ├─▶ If the write succeeds, ACK all corresponding messages (step 5a).
   └─▶ If the write fails, NACK all corresponding messages (step 5b).
   
5a. ACK SUCCESSFUL WRITES
    └─▶ All messages in the successfully written batch are ACK'd with NATS.
    
5b. NACK FAILED WRITES  
    └─▶ All messages from the failed write batch are NACK'd with NATS for redelivery.
```

This synchronous flow ensures that a worker is fully occupied with one batch at a time, simplifying state management and error handling.

### 3. Message States

The message state lifecycle remains the same, but it's important to note that the entire `PROCESSING` step is synchronous and blocking within the worker.

```
                    ┌─────────────┐
                    │   PENDING   │ (in NATS)
                    └──────┬──────┘
                           │ Fetch Batch
                    ┌──────▼──────┐
                    │ PROCESSING  │ (Synchronous work in a single worker)
                    └──────┬──────┘
                      ┌────┴────┐
              Parse   │         │   Write
              Fail    │         │   Success
                      ▼         ▼
               ┌──────────┐ ┌──────────┐
               │   NACK   │ │   ACK    │
               │ (retry)  │ │ (done)   │
               └────┬─────┘ └──────────┘
                    │
                    └────▶ Back to PENDING
```

### 4. Failure Handling

#### Parse Failures
- Individual messages that fail parsing are immediately NACKed
- They return to NATS for retry (up to MaxDeliver limit)
- Other messages in the batch continue processing
- No "poisoning" logic - messages retry until MaxDeliver

#### Write Failures  
- ALL messages in the write batch are NACKed together
- Maintains transactional semantics (all-or-nothing)
- Circuit breaker prevents cascading failures
- Messages retry with exponential backoff

### 5. Circuit Breaker Pattern

Each worker has its own circuit breaker:

```
CLOSED (normal)           OPEN (blocking)
    │                         │
    │ 5 failures ──────▶      │
    │                         │
    │      ◀────── 30s        │
    │              timeout    │
    │                         ▼
    │              ┌──────────────────┐
    │              │   HALF-OPEN      │
    │ ◀─────────── │  (testing)       │
    │ 2 successes  └──────────────────┘
```

### 6. Multiple Workers & Scaling

The system is scaled horizontally by running multiple independent connector processes. These processes are the "workers".

```
                                     +-------------------------+
                                     |   NATS JetStream Server |
                                     |                         |
                                     |  Durable Consumer:      |
                                     |  `my-durable-processor` |
                                     +-----------▲-------------+
                                                 |
          +--------------------------------------+--------------------------------------+
          | fetch()                              | fetch()                              |
          |                                      |                                      |
+---------▼---------+                  +---------▼---------+                  +---------▼---------+
|  Worker Process 1 |                  |  Worker Process 2 |                  |  Worker Process 3 |
| (gets batch #1)   |                  | (gets batch #2)   |                  | (gets batch #3)   |
|                   |                  |                   |                  |                   |
|  ┌─────────────┐  |                  |  ┌─────────────┐  |                  |  ┌─────────────┐  |
|  │     CB      │  |                  |  │     CB      │  |                  |  │     CB      │  |
|  └──────┬──────┘  |                  |  └──────┬──────┘  |                  |  └──────┬──────┘  |
|         │         |                  |         │         |                  |         │         |
|         ▼         |                  |         ▼         |                  |         ▼         |
+---------+---------+                  +---------+---------+                  +---------+---------+
          |                                      |                                      |
          +--------------------------------------+--------------------------------------+
                                                 |
                                          ┌──────▼──────┐
                                          │  QuasarDB   │
                                          └─────────────┘
```

Each worker process:
- Is a separate OS process (e.g., a Kubernetes pod).
- Connects to the same durable consumer name in NATS.
- Receives a unique batch of messages from JetStream for processing.
- Has its own circuit breaker instance.
- Processes its batch synchronously and independently.
- Can fail and recover without impacting other workers.

This architecture provides natural load distribution and horizontal scalability, managed by NATS JetStream itself. There is no in-process sink or complex asynchronous hand-off.

## Consequences

### Positive
- **Simple to understand**: Clear 6-step process
- **Resilient**: Handles parse and write failures differently
- **High throughput**: Parallel workers with batching
- **No data loss**: Explicit ACK only after successful write
- **Gradual degradation**: Per-worker circuit breakers
- **Observable**: Metrics for monitoring health

### Negative  
- **Parse retry overhead**: Bad messages retry until MaxDeliver
- **No partial batch writes**: One bad write NACKs all
- **Memory usage**: Each worker maintains its own state

### Trade-offs
We chose simplicity over complex retry logic:
- No message poisoning (rely on NATS MaxDeliver)
- No partial batch processing for writes
- Clear operational semantics over edge case optimization

## Metrics

The system tracks:
- `messagesProcessed`: Successfully written messages
- `parseFailures`: Messages that failed parsing
- `writeFailures`: Batches that failed writing

## Configuration

Key parameters:
- `BatchSize`: Messages per fetch (default: 100)
- `MaxDeliver`: NATS retry limit (default: 10)
- `NumWorkers`: Parallel workers (default: 4)
- `CircuitBreakerFailureThreshold`: Failures to open (default: 5)
- `CircuitBreakerSuccessThreshold`: Successes to close (default: 2)
- `CircuitBreakerTimeout`: Recovery attempt delay (default: 30s)

## References
- [Circuit Breaker Pattern](https://martinfowler.com/bliki/CircuitBreaker.html)
- [NATS JetStream Documentation](https://docs.nats.io/jetstream)
- [QuasarDB Writer API](https://doc.quasardb.net/)