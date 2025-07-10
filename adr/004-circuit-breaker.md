# ADR-004: Circuit Breaker Pattern for Service Protection

## Status
Proposed

## Context
The NATS connector needs to handle failures when writing to QuasarDB. Currently, each worker independently retries failed operations, which can overwhelm a struggling service and delay recovery.

## Decision
Implement the Circuit Breaker pattern with shared state across workers to provide coordinated failure handling and recovery. The circuit breaker will be located in `connector/resilience/` as it is inherently tied to worker coordination.

## Rationale

### Why Not Just Retry + Backoff?

**Traditional Retry (Per-Worker):**
```
Worker 1: [Fail]--1s-->[Retry]--2s-->[Retry]--4s-->[Retry]...
Worker 2: [Fail]--1s-->[Retry]--2s-->[Retry]--4s-->[Retry]...
Worker 3: [Fail]--1s-->[Retry]--2s-->[Retry]--4s-->[Retry]...
...
Worker N: [Fail]--1s-->[Retry]--2s-->[Retry]--4s-->[Retry]...

Result: N workers × M retries = N×M requests hitting failing service
```

**Circuit Breaker (Shared State):**
```
Worker 1-5: [Fail] [Fail] [Fail] [Fail] [Fail]
            |
            v
    Circuit Opens (Shared Decision)
            |
            v
Worker 6-100: [Instant Fail - No Network Call]
            |
        After 30s
            v
    Circuit Half-Open
            |
            v
Worker X: [Test Request]--Success-->[Allow More]
```

### Key Benefits

1. **Coordinated Protection**: One decision affects all workers
2. **Fast Fail**: Better user experience during outages
3. **Predictable Recovery**: Service gets dedicated recovery time
4. **Resource Efficiency**: No wasted network calls during outages

### Circuit Breaker States

```
        +----------+
        |  CLOSED  |<---------+
        +----------+          |
             |                |
     Failure Threshold        |
             |          Success Threshold
             v                |
        +----------+          |
        |   OPEN   |          |
        +----------+          |
             |                |
         Timeout              |
             |                |
             v                |
        +----------+          |
        |HALF-OPEN |----------+
        +----------+
             |
         Any Failure
             |
             v
        Back to OPEN
```

### Progressive Half-Open Recovery

To prevent thundering herd when recovering:
```
HALF-OPEN State:
  Allow 1 request → Success → Allow 2 → Success → Allow 4 → ...
  
  Exponential increase until fully recovered (32 consecutive successes)
  Any failure → Back to OPEN state
```

### Shared State Architecture

```
                    +-------------------+
                    | Circuit Breaker   |
                    | Manager (Shared)  |
                    +-------------------+
                            |
                 +----------+----------+
                 |                     |
        +--------v--------+   +--------v--------+
        | Circuit Breaker |   | Circuit Breaker |
        | (qdb-cluster)   |   | (nats-server)   |
        +--------^--------+   +--------^--------+
                 |                     |
        +--------+--------+   +--------+--------+
        |                |   |                |
    Worker 1-50      Worker 51-100    (All Workers)
```

### Hook Integration

Circuit breaker state changes are observable via the existing hooks system:
```go
type CircuitBreakerStateChange struct {
    WorkerID    string
    Resource    string    // "qdb-cluster", "nats-server"
    OldState    string    // "closed", "open", "half-open"
    NewState    string
    Reason      string    // "threshold exceeded", "recovery complete"
    Timestamp   time.Time
    
    // Optional context
    FailureCount int      // For closed→open transitions
    SuccessCount int      // For half-open→closed transitions
    Error        error    // Last error if applicable
}
```

Single hook design chosen for:
- State transitions are inherently from→to events
- Subscribers typically want all transitions with context
- Simpler registration (1 hook vs 6+ state-specific hooks)
- Aligns with circuit breaker libraries (Hystrix, resilience4j)

## Implementation Location

Circuit breakers will be implemented in `connector/resilience/` because:
1. **Shared State Reality**: Workers must coordinate through shared state
2. **Hook Integration**: Direct integration with connector's hook system
3. **Worker-Specific Logic**: Jitter and recovery patterns are worker-specific
4. **Semantic Clarity**: Not a general utility, but worker-coordination specific

## Consequences

### Positive
- Prevents cascading failures
- Reduces load on failing services
- Provides predictable recovery windows
- Improves overall system resilience
- Observable state changes via hooks

### Negative
- Requires shared state management
- May block some requests that could succeed
- Adds complexity compared to simple retry
- Needs careful tuning of thresholds

### Neutral
- Changes failure behavior from gradual degradation to binary (working/not working)
- Requires monitoring to understand circuit state (via hooks)
- May need per-resource circuit breakers (QDB, NATS separately)