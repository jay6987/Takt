# Pipe Lifecycle and Semantics

## Mental Model

A `Pipe<T>` is a fixed-capacity, preallocated circular belt.

- Capacity is fixed at construction.
- Baskets are reused, not dynamically allocated per transfer.
- Writers and readers are blocked by condition variables.

## States

1. Open and writable/readable
2. Open but blocked (writer waits when no free slot, reader waits when no data)
3. Closed (no new write token can be acquired)
4. Closed and empty (readers receive `PipeClosedAndEmptySignal`)

## Write Side

- `acquire_write(is_segment_end)` acquires one basket and marks whether this token ends a segment.
- `acquire_write_batch(count, is_segment_end)` acquires contiguous logical write work in one token.
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
- `overlap_size < read_size` must hold.
- Overlap reads require one previous completed read batch.
- Read windows are bounded by committed segment ends and do not cross segments.
- Read tokens expose `is_segment_start()` and `is_segment_end()` metadata.

## Backpressure Rule

Backpressure is intentionally centralized in the pipe.

- Producers block when capacity is exhausted.
- Consumers block when data is unavailable.
- Node includes an internal task queue for manager-to-worker dispatch.
- End-to-end pacing remains constrained by pipe capacity and token flow.

## Close Protocol

Producer side usually calls `close()` when no more data will be written.

Consumer loops should catch `PipeClosedAndEmptySignal` and exit gracefully.

In addition, ordered write commit failure can also force the pipe into a closed terminal path.

## Fault Behavior

- Root cause: a write token was released without `publish()`.
- First-order consequence: the pipe becomes faulted and closed for future writes.
- Future write acquisitions throw `PipeFaultedError`.
- Readers may still drain the already contiguous visible prefix, then receive `PipeClosedAndEmptySignal`.
