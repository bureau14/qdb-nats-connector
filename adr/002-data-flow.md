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
We implement a simplified batch processing pipeline with the following characteristics:

### 1. Batch Processing Flow

```
┌─────────────┐     ┌──────────────┐     ┌─────────────┐     ┌────────────┐
│    NATS     │────▶│    Worker    │────▶│   Parser    │────▶│  QuasarDB  │
│  JetStream  │     │  (fetches)   │     │   (JSON)    │     │   (sink)   │
└─────────────┘     └──────────────┘     └─────────────┘     └────────────┘
       │                    │                     │                   │
       │◀───────────────────┴─────────────────────┴───────────────────┘
                              ACK/NACK
```

### 2. Processing Steps

The worker follows a 6-step process for each batch:

```
1. FETCH BATCH
   └─▶ Get messages from NATS (typically 10-100 messages)
   
2. PARSE MESSAGES  
   ├─▶ Valid messages → continue to step 5
   └─▶ Failed messages → NACK immediately (step 3)
   
3. NACK PARSE FAILURES
   └─▶ Failed messages return to NATS for retry
   
4. CHECK VALID COUNT
   └─▶ If no valid messages, return (done)
   
5. WRITE TO QUASARDB (with circuit breaker)
   ├─▶ Success → ACK messages (step 6a)
   └─▶ Failure → NACK messages (step 6b)
   
6a. ACK SUCCESSFUL WRITES
    └─▶ Messages removed from NATS
    
6b. NACK FAILED WRITES  
    └─▶ Messages return to NATS for retry
```

### 3. Message States

```
                    ┌─────────────┐
                    │   PENDING   │ (in NATS)
                    └──────┬──────┘
                           │ Fetch
                    ┌──────▼──────┐
                    │ PROCESSING  │ (in Worker)
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

### 6. Multiple Workers

```
┌─────────────────────────────────────────┐
│               NATS JetStream            │
└────┬──────────┬──────────┬──────────┬──┘
     │          │          │          │
  Worker-0   Worker-1   Worker-2   Worker-3
     │          │          │          │
     ▼          ▼          ▼          ▼
  ┌─────┐    ┌─────┐    ┌─────┐    ┌─────┐
  │ CB  │    │ CB  │    │ CB  │    │ CB  │  (Circuit Breakers)
  └──┬──┘    └──┬──┘    └──┬──┘    └──┬──┘
     │          │          │          │
     └──────────┴──────────┴──────────┘
                     │
              ┌──────▼──────┐
              │  QuasarDB   │
              │    Sink     │
              └─────────────┘
```

Each worker:
- Has its own circuit breaker instance
- Processes batches independently  
- Can fail/recover independently
- Provides natural load distribution

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