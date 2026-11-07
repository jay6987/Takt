# Pipe Lifecycle and Semantics

## Mental Model

A `Pipe<T>` is a stable connection object that becomes a fixed-capacity,
preallocated circular belt only after explicit initialization.

- Construction stores the name but allocates no runtime baskets.
- Both ends configure the same object; equal names do not share state.
- Capacity, typed producer template, descriptors and history policy are
  configured before initialization. Capacity is fixed once initialized.
- Baskets are reused, not dynamically allocated per transfer.
- Writers and readers are blocked by condition variables.

## Preparation Lifecycle

```text
Configuring --initialize()--> Ready --close()--> Closed
                                   --close_with_error()--> Faulted
Configuring --close()/close_with_error()--> Cancelled
```

Both producer and consumer declarations are mandatory. Repeating either
declaration throws; initialization freezes all configuration. Repeating
`initialize()` throws rather than replacing resources. Initialization has a
strong exception guarantee: failed allocation/copy/validation leaves the
configuration intact and no partial runtime; it can be corrected and retried.
The producer template is available via `basket_template()` during configuration,
without consulting a runtime basket. Producer/consumer config and descriptor
queries remain available after initialization.

Before initialization, `size()` is zero, `closed()` and `faulted()` are false,
and `fault_message()` is empty unless cancelled/faulted explicitly. `capacity()`
returns the explicit capacity when set, otherwise resolves the two configured
ends or throws for missing configuration/overflow. Every acquisition entry
point throws before initialization instead of blocking.

Configuration-time `close()` safely cancels without allocating baskets;
`close_with_error(message)` also preserves the first nonempty failure.
Empty error messages are rejected. Cancellation prevents further configuration
and initialization and does not turn uninitialized reads into normal EOF.
An uninitialized pipe can be destroyed safely.

`PipelineRuntime::initialize()` prepares all collected connection objects and
nested runtimes before any workers start. `start()` requires preparation; it
never initializes implicitly. Configure/initialize before any prefill or close.
Shared Direct subgraph boundaries refer to this same connection object; recursive
preparation deduplicates it by identity, not name. Only the real compute producer
and consumer configure it. Direct removes the additional bridge queue and its
capacity; all boundary pipes remain registered for stop even without workers.
See [Pipe Usage](pipe-usage.md) for capacity formulas and API migration.

`PipelineRuntime::request_stop()` closes its registered pipes as well as stopping
nodes and nested runtimes, even when a node has no pipe-closing override.
Stopping a nonfailed runtime makes it terminal (`Stopped`): later initialization
or start is rejected. Stopping before initialization cancels uninitialized
registered pipes without allocating baskets. Cleanup via repeated stop/join
remains safe; a failed runtime preserves its original failure.

## States

1. Open and writable/readable
2. Open but blocked (writer waits when no free slot, reader waits when no data)
3. Closed (no new write token can be acquired)
4. Closed and empty (readers receive `PipeClosedAndEmptySignal`)
5. Faulted and drained (readers receive `PipeFaultedError`)

## Write Side

- `acquire_write(is_segment_end)` acquires one basket and marks whether this token ends a segment.
- `acquire_write_batch(count, is_segment_end)` acquires contiguous logical write work in one token.
- `acquire_configured_write_batch(is_segment_end)` uses the producer's `write_size`.
- `publish()` marks the token as committed, but read visibility still follows acquire order.
- A later published token does not become visible until all earlier write tokens are released and published.
- A published write token that remains alive still holds capacity and does not complete visibility progression until it is released.
- If an earlier write token is released without `publish()`, the pipe enters a sticky fault path and later reserved writes are not exposed to readers.
- Once faulted, future write acquisition throws `PipeFaultedError`.
- Fault status can be queried with `faulted()` and `fault_message()`.
- Write tokens expose `is_segment_start()` and `is_segment_end()` metadata.

## Read Side

- `acquire_read()` acquires one readable element.
- `acquire_read_batch(read_size, overlap_size)` supports sliding-window style consumption.
- `acquire_configured_read_batch()` uses the consumer's configured window/overlap.
- `overlap_size < read_size` must hold.
- Overlap reads require one previous completed read batch.
- Read windows are bounded by committed segment ends and do not cross segments.
- Read tokens expose `is_segment_start()` and `is_segment_end()` metadata.
- Multiple active read tokens reserve successive segments without releasing
  earlier baskets. A basket cannot be reused until its read token is destroyed.
- `ReadHistoryPolicy::Snapshot` is the backward-compatible default and copies
  completed read payloads to support overlap. Use `ReadHistoryPolicy::Disabled`
  for ordinary frame pipelines: release does not copy payloads, and overlap
  requests fail explicitly.
- Nontrivial payloads such as `std::vector<float>` need no codec for ordinary
  operation. A codec is required only when record/replay is bound.

## Backpressure Rule

Image backpressure is centralized in the pipe; task admission is bounded too.

- Producers block when capacity is exhausted.
- Consumers block when data is unavailable.
- Node includes an internal task queue for manager-to-worker dispatch.
- Its default capacity is one task. At most `worker_count + queue_capacity + 1`
  tasks can hold tokens, including a manager currently constructing a task.
- End-to-end pacing remains constrained by pipe capacity and token flow.

## Close Protocol

Producer side usually calls `close()` when no more data will be written.
Close rejects new write reservations, but existing writers may finish.
Readers wait for those reservations before reporting terminal status. An
unterminated successful segment prefix is sealed for draining during closure.

Consumer loops should catch `PipeClosedAndEmptySignal` and exit gracefully.

In addition, ordered write commit failure can also force the pipe into a closed terminal path.

## Fault Behavior

- Root cause: a write token was released without `publish()`, or a producer
  explicitly called `close_with_error(message)`.
- First-order consequence: the pipe becomes faulted and closed for future writes.
- Future write acquisitions throw `PipeFaultedError`.
- Readers drain the successfully published contiguous prefix, then receive
  `PipeFaultedError`, not a success-shaped normal end-of-stream.
- Already reserved writers may finish after explicit error closure. After an
  abandoned reservation, however, later logical frames are never exposed across
  that gap. In both cases the first error is retained.
- `fault_message()` returns a synchronized snapshot. Empty explicit messages
  are rejected.
