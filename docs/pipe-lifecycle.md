# Pipe Lifecycle and Semantics

## Mental Model

A `Pipe<T>` is a stable connection object that becomes a fixed-capacity,
preallocated circular belt only after explicit initialization.

- Construction stores the name but allocates no runtime frames.
- Both ends configure the same object; equal names do not share state.
- Slot capacity, typed producer frame template, and descriptors are
  configured before initialization. Capacity is fixed once initialized.
- Frame slots are reused, not dynamically allocated per transfer.
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
The producer template is available via `frame_template()` during configuration,
without consulting runtime frame storage. Producer/consumer config and descriptor
queries remain available after initialization.

Before initialization, `queued_frame_count()` is zero, `closed()` and `faulted()`
are false, and `fault_message()` is empty unless cancelled/faulted explicitly.
`slot_capacity()` returns the explicit capacity when set, otherwise resolves the two configured
ends or throws for missing configuration/overflow. Every acquisition entry
point throws before initialization instead of blocking.

Configuration-time `close()` safely cancels without allocating frames;
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
registered pipes without allocating frames. Cleanup via repeated stop/join
remains safe; a failed runtime preserves its original failure.

## States

1. Open and writable/readable
2. Open but blocked (writer waits when no free slot, reader waits when no data)
3. Closed (no new write token can be acquired)
4. Closed and empty (readers receive `PipeClosedAndEmptySignal`)
5. Faulted and drained (readers receive `PipeFaultedError`)

## Write Side

- `acquire_write()` reserves the configured `frames_per_write` and does not end the Shot.
- `acquire_shot_end_write(count)` reserves 1 through `frames_per_write` slots and
  explicitly ends the Shot.
- Acquiring a count above `frames_per_write` or available capacity throws instead of waiting forever.
- `publish()` marks the token as committed, but read visibility still follows acquire order.
- A later published token does not become visible until all earlier write tokens are released and published.
- A published write token that remains alive still holds capacity and does not complete visibility progression until it is released.
- If an earlier write token is released without `publish()`, the pipe enters a sticky fault path and later reserved writes are not exposed to readers.
- Once faulted, future write acquisition throws `PipeFaultedError`.
- Fault status can be queried with `faulted()` and `fault_message()`.
- Write tokens expose `is_shot_start()` and `is_shot_end()` metadata.
- `close()` closes the pipe and wakes waiters before reporting an unfinished Shot
  with `PipeUnterminatedShotError`. It does not create a ShotEnd. Published
  data from the unfinished Shot remains drainable; its last read window is not
  marked as a ShotEnd, then readers receive `PipeClosedAndEmptySignal`.

## Read Side

- `acquire_read()` uses the configured consumer window and returns a move-only
  read token.
- Use `value(index)` to access window elements; indexless access is valid only
  when the returned window has one value.
- `overlap_frames < frames_per_read` must hold.
- The window advances by `frames_per_read - overlap_frames`.
- During an open Shot, a window becomes readable once its full stride of newly
  published slots is available; it does not wait for ShotEnd.
- Read windows are bounded by committed Shot ends and do not cross Shots;
  ShotEnd may return a shorter final window.
- Read tokens expose `is_shot_start()` and `is_shot_end()` metadata.
- Multiple active read tokens reserve successive windows without releasing
  earlier frames. Active tokens pin their slots until destroyed.
- Overlap slots stay in the ring buffer until they are no longer needed. The
  first read in each Shot has zero overlap; overlap never crosses a Shot
  end.
- Overlapping tokens can reference the same ring slots. Treat overlap values as
  read-only while another active token may be using them.
- Nontrivial payloads such as `std::vector<float>` need no codec for ordinary
  operation. A codec is required only when record/replay is bound.

## Backpressure Rule

Image backpressure is centralized in the pipe, and task admission reserves
workers before task construction.

- Producers block when capacity is exhausted.
- Consumers block when data is unavailable.
- Node has no manager-to-worker backlog. Ingress reserves an idle worker before
  `build_task`, so at most `worker_count` built tasks can hold tokens.
- End-to-end pacing remains constrained by pipe capacity and token flow.

`Node` no longer accepts `task_queue_capacity`; its constructor is
`Node(name, worker_count = 1)`. To group several tasks over one token-owned
frame, retain the context on ingress and mark the final task with
`Task::barrier`. The successful `on_barrier_complete(sequence_id)` hook runs
after the barrier and all earlier task payloads have been destroyed. Publish
the output and release the context there; a no-op barrier task represents an
empty group. See [Node Usage](node-usage.md#grouped-work-with-barriers) for a
vector-region example.

## Close Protocol

Producer side usually calls `close()` when no more data will be written.
Close rejects new write reservations, but existing writers may finish.
Readers wait for those reservations before reporting terminal status. An
unfinished Shot's published prefix remains drainable during closure without
being marked as a ShotEnd.

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
