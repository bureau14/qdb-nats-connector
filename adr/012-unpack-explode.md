# ADR-012: Array ingestion -- `unpack` and `explode` transformation steps

## Status

Accepted (2026-07-10)

## Context

Condition-monitoring feeds (reference case: GeorgiaPacific's SKF
ion-stream, capability 4) carry waveforms as packed fixed-width binary
sample arrays plus a per-message amplitude scale, with no sample rate in
the payload. QuasarDB stores waveforms as one row per sample --
`$timestamp[i] = start + i*interval`, scalar metadata broadcast on every
row -- to enable server-side aggregation.

Two generic primitives close the gap:

- **`unpack`**: reinterpret a `[]byte` field as a typed numeric array.
  Vocabulary rule: `parse_*` steps handle self-describing formats (schema
  in the data: JSON, protobuf); `unpack` handles formats whose schema
  lives in the config because raw bytes cannot carry it. The name is the
  cross-language term of art (Python `struct.unpack`, Erlang bit syntax).
- **`explode`**: the single 1->N primitive (Spark/Hive `explode`, SQL
  `UNNEST WITH ORDINALITY`), owning time-axis reconstruction over the
  0-based ordinal.

Alternatives (mid-pipeline plural state, dedicated waveform parser,
source-side splitting, fused mega-step, N single-row tables per message,
separate array-op step, other names) were evaluated and rejected in a
multi-perspective design review (2026-07-09); see the rationale summary
below and do not relitigate.

## Decisions

### 1. Terminal explode

The transformation language stays 100% scalar; arrays are just typed
field values. Cardinality changes exactly once, at row materialization:
`explode` is not a `TransformationStep`, never enters the pipeline, must
be the last spec (compile-time enforced), and at most one may appear per
config. `Parse` dispatches to an N-row builder when an explode is
compiled. Precedent: Postgres deprecated SRFs-in-SELECT, Spark allows one
generator per SELECT, ClickHouse confines fan-out to `ARRAY JOIN`.

`explode` and `extract_index` are mutually exclusive: explode owns the
`$timestamp` index via `index.start` (a `time.Time` field, e.g. from
`extract_timestamp`) and `index.interval`. The `time.Now()` index
fallback is unreachable for explode configs.

### 2. `unpack` is structural; full fixed-width type family

Decode is all-or-nothing: a missing/mistyped source, a byte length that
is not a multiple of the element width, or an invalid scale is
`OutcomeUnusable` (zero tables) -- a partially-decoded sample array has
no meaningful sentinel form.

`type` covers every Go-native fixed width: int8/16/32/64,
uint8/16/32/64, float32/64. `endianness` is optional, defaulting to the
host byte order (`binary.NativeEndian`); cross-machine wire formats
should declare it explicitly. Output typing is strict and mirrors the
protobuf decoder's conventions:

- integer types without scale -> `[]int64` (uint64 wraps above MaxInt64,
  two's complement, same as proto's `int64(v.Uint())`);
- float types without scale -> `[]float64` (f32 widened exactly);
- any type with `scale` -> `[]float64`, computed as **widen the element
  to float64 FIRST, then multiply by the float64 scale** -- the
  arithmetic contract golden diffs rely on. For uint64 the widening
  preserves the unsigned value: the declared element type, not the
  output container, defines the element's value.

`scale` takes exactly one of `value` (literal) or `source` (dot-path to
a `float64`).

### 3. Scale NaN/Inf/0.0 are structural failures

A NaN scale would write N doubles that ARE QuasarDB's double-null
sentinel (`QDB_IS_NULL_DOUBLE -> isnan`): silent total data loss no
other layer can catch. 0.0 is protobuf's absent-field default, making it
the likeliest real corruption signature. Evidence: 0 of 224 captured SKF
waveform messages carry a 0.0 scale (verified 2026-07-10). All three are
rejected per message (sourced) and at config load (literal).

### 4. Empty array -> zero rows, OutcomeOK, counted

The ecosystem splits here (Spark drops the record, pandas fabricates a
null row); a fabricated null-amplitude sample at t0 is worse than no
row. An empty source array short-circuits BEFORE start/interval
resolution (zero rows need no time axis), returns zero tables with
`OutcomeOK`, is ACKed like any valid parse, and increments the worker's
`parses_zero_rows_total` counter so silent data disappearance stays
observable. Named future opt-in, specified but NOT implemented:
`on_empty: null_row|drop`.

### 5. Interval sourcing: `value | source+unit | by_length`, no default, no anchor

`index.interval` takes exactly one of:

- `value`: static Go duration string, parsed and validated positive at
  config load;
- `source`: dot-path to an int64/float64 field plus a **mandatory**
  `unit` (`ns|us|ms|s`). A unitless source is a compile-time error:
  wrong-unit x 8192 samples = years-off but plausible-looking
  timestamps, the worst failure class in a tsdb;
- `by_length`: a map from array length to duration, letting one consumer
  serve populations with different rates on the same subjects (SKF: 8192
  vs 3200 samples). A length with no entry is a structural per-message
  failure -- never guess a time axis.

There is deliberately NO default (unlike `extract_table`'s `$table`
source default) and NO anchor option: `index.start` marks the
capture-window START, `t[i] = start + i*interval`, `t[0] == start`
exactly.

Timestamps are computed by integer-nanosecond MULTIPLICATION per
ordinal, never accumulation (float accumulation drifts at non-dyadic
rates). Config durations are inherently integer-ns quantized
(`time.Duration`): exact 3 kHz is not representable and must be
approximated (e.g. `333333ns`); this quantization is accepted and
documented rather than hidden behind rational-arithmetic complexity.

### 6. Broadcast rule and the widened sentinel-fill invariant

Every output column is statically classified at config load: _exploded_
(the explode `target`, bound per-element, or the optional 0-based int64
`ordinal`) or _broadcast_ (scalar field replicated N times). An
array-typed field reaching an output column without an explode binding
is a config-load error (enforced where statically knowable: every
`unpack` target naming an output column must be the explode source; the
per-message type assertion remains authoritative).

The single-row sentinel-fill invariant (ADR-005) widens from 1 to N: its
content is alignment, not one-ness. Every output column receives exactly
N values; a missing broadcast field fills N copies of the per-type null
sentinel (`OutcomePartial`, unchanged in kind). The N-row builder
self-asserts `len(column) == N` before every SetData because
`MergeSingleTableWriters` does NOT validate per-column lengths -- a
violation would write silently misaligned (timestamp, value) pairs.
Per-row failure is impossible by construction: decode is all-or-nothing
and scale/start/interval are broadcast.

Static type trace: when the exploded source is an `unpack` target in the
same config, the unpack output type is validated against the target
column type at load.

### 7. Filters cannot reference exploded columns

`RowFilter.Apply` evaluates row 0 only -- exact for broadcast columns,
silently wrong for per-sample ones. `filter.New` therefore rejects specs
referencing the exploded column set (target + ordinal) at config load.
Per-row filtering (e.g. dropping clipping sentinels) is a named future
extension.

### 8. Pre-specification: multi-array explode

Multiple parallel arrays later only via strict equal-length zip (the
ClickHouse `ARRAY JOIN` rule); ragged lengths = structural failure, zero
rows; NEVER a cross-product. Out of scope now, pre-specified to prevent
redesign.

### 9. Deferred: expected-drop observability

Mixed-capability subjects make structural drops routine (each consumer
drops the other capabilities: the SKF waveform consumer drops ~96% of
messages at `extract_map_entry allowed_keys`). Interim guidance lives in
the consumer configs/runbooks: alert on `parse_failures`/`nacks` and on
`messages_dropped_total` as a ratio of `messages_fetched_total`, never
on drops > 0. The future mechanism -- a typed AllowedKeysDrop error from
`extract_map_entry` plus a `drops_expected_total` counter -- is deferred
to the QDB-19373 follow-up.

## Operational notes

- Worst-case fetch batch (100 messages x 8192 samples ~ 819k rows,
  ~90 MiB heap measured) writes in ~2 s against a local cluster;
  waveform consumers should still run `--nats-batch-size 10..25` because
  a whole-batch NACK on write failure re-parses and re-explodes
  everything (redelivery amplified by the sample count).
- Broadcast string/symbol columns repeat one string header N times;
  `runtime.Pinner` pin counts nest, verified under
  `GOEXPERIMENT=cgocheck2 -race` by the waveform integration tests.
- Open item carried: the real SKF sample rates for the 8192/3200
  populations are pending GP confirmation; the local config ships
  placeholder `by_length` intervals (200us / 312.5us).

## Testing

Unit: per-type golden vectors and rapid generative round-trips against
`encoding/binary` (unpack); compile-time validation matrix, exact-
multiplication and monotonicity properties, zero-row/structural/partial
classification (explode). Integration (real QDB, cgocheck2 + race):
merged N-row/single-row readback alignment, ~819k-row batch push,
broadcast string/symbol pinning. Regression: zero-table OK parses ACK
and count. Golden: 224 captured SKF waveforms diff bit-exact (values)
and ns-exact (timestamps) against an independent Python reference
decoder (1,275,904 rows).

---

_This ADR extends ADR-005 (parser architecture); the single-row
materialization language there is superseded by the widened invariant in
Decision 6 for explode configs._
