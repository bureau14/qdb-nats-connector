# ADR-003: Hook-Based Failure Injection and Observability

## Status
Proposed

## Context
To ensure the reliability of our checkpointing mechanism, we need to test recovery scenarios where the connector fails at various stages of message processing. We need to verify that:
- Messages are correctly replayed after failures
- Deduplication modes (disabled, drop, upsert) behave correctly during replay
- No data loss occurs during crashes

Additionally, we anticipate future needs for:
- Metrics collection (latency, throughput, error rates)
- Distributed tracing integration
- Custom logging and alerting
- Performance profiling

## Decision
We will implement a lightweight, functional hook system that allows registering callbacks at key points in the message processing pipeline. These hooks will serve dual purposes:
1. **Testing**: Inject failures at precise points for integration tests
2. **Observability**: Collect metrics, traces, and logs in production

The hook system will:
- Use simple function types (`HookFunc`) rather than interfaces
- Support multiple hooks per injection point
- Execute all hooks synchronously (fail-fast on error)
- Enforce timing thresholds with warnings (not errors):
  - Pre* hooks: 100μs threshold
  - Post* hooks: 500μs threshold
- Pass strongly-typed data structures to avoid interface{} casting
- Hook implementors must ensure fast execution (use channels/buffering for expensive operations)

Hook points will be placed at:
- **PreRead**: Before fetching messages from NATS
- **PostRead**: After fetch completes (success or failure)
- **PreWrite**: Before writing to QuasarDB
- **PostWrite**: After write completes
- **PreAck**: Before acknowledging messages
- **PostAck**: After acknowledgment completes

## Consequences

### Positive
- **Minimal production impact**: Hooks are optional and have negligible overhead when not registered
- **Reusable infrastructure**: Same system supports testing, metrics, logging, and tracing
- **Type safety**: Strongly-typed hook data prevents runtime errors
- **Clean separation**: Worker code remains focused on business logic
- **Future extensibility**: Easy to add new hook points without breaking changes
- **Testing confidence**: Precise failure injection enables thorough recovery testing
- **Simplified implementation**: No goroutines, channels, or data copying concerns
- **Predictable behavior**: Synchronous execution makes debugging straightforward
- **Performance visibility**: Timing warnings identify problematic hooks

### Negative
- **Additional complexity**: Adds another abstraction layer to understand
- **Blocking risk**: Poorly implemented hooks can slow processing (mitigated by timing warnings)
- **Hook ordering**: Multiple hooks execute in registration order, which may cause subtle dependencies
- **No fire-and-forget**: Expensive operations must be explicitly made async by hook implementors

### Neutral
- **Not a full event system**: Intentionally lightweight, not meant for complex event processing
- **No built-in persistence**: Hooks are in-memory only, not durable across restarts

## Alternatives Considered

1. **Interface-based mocking**: Would require refactoring production code to use interfaces everywhere
2. **Process-level testing**: Less deterministic, harder to control failure timing
3. **OpenTelemetry hooks**: Too heavyweight for our current needs, but we could integrate later
4. **No hooks**: Would make comprehensive testing extremely difficult

## Implementation Notes
- Hooks live in `connector/hooks/hooks.go` as a dedicated package
- Worker modifications are minimal - just hook registry injection and execution calls
- Hook execution includes automatic timing measurement and warnings
- Test-specific hooks live in test packages, not production code
- Metrics hooks can be added by implementors using channels for async processing