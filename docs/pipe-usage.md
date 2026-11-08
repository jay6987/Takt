# Pipe Usage Guide

This page shows the minimal usage pattern for `Pipe<T>`.

## Single Producer and Consumer

```cpp
takt::Pipe<int> numbers("numbers");
numbers.set_producer({"source", takt::concurrency(1), takt::frames_per_write(1)}, 0);
numbers.set_consumer(
    {"sink", takt::concurrency(1), takt::frames_per_read(1), takt::overlap_frames(0)});
numbers.set_slot_capacity(8);
numbers.initialize();

for (int i = 1; i <= 5; ++i)
{
    auto w = numbers.acquire_shot_end_write(1);
    w.value() = i;
    w.publish();
}
numbers.close();

while (true)
{
    try
    {
        auto r = numbers.acquire_read();
        // consume r.value()
    }
    catch (const takt::PipeClosedAndEmptySignal&)
    {
        break;
    }
}
```

## Key Rules

- Construct one pipe per connection and share that same object with both ends.
  Names are diagnostic labels, not registry keys.
- Configure both ends and the producer's typed frame template before
  `initialize()`. Either end may configure first; configuration is coordinated
  serially, not concurrently with initialization or workers.
- `ProducerConfig` fields are `name`, `concurrency`, `frames_per_write`;
  `ConsumerConfig` fields are `name`, `concurrency`, `frames_per_read`, `overlap_frames`.
- Optional `set_descriptor()` also belongs to configuration. After
  initialization, all configuration is frozen.
- A standalone pipe must be explicitly initialized before any token acquisition.
- Always call `publish()` on write tokens that should become visible.
- Call `close()` when no more data will be written. If the Shot is unfinished,
  `close()` closes and wakes the pipe before throwing
  `PipeUnterminatedShotError`; it does not fabricate a ShotEnd.
- Consumer loops should exit on `PipeClosedAndEmptySignal`.

## Named Configuration Arguments

Configuration constructors require distinct strong types, in this fixed order:

```cpp
// Producer: name, concurrency, frames per write
numbers.set_producer({"source", takt::concurrency(2), takt::frames_per_write(4)}, 0);
// Consumer: name, concurrency, frames per read, overlap
numbers.set_consumer(
    {"sink", takt::concurrency(3), takt::frames_per_read(5), takt::overlap_frames(2)});
```

`concurrency()` returns `Concurrency`, `frames_per_write()` returns
`FramesPerWrite`, `frames_per_read()` returns `FramesPerRead`, and `overlap_frames()`
returns `OverlapFrames`. Each wraps a `size_t` without implicit conversions.
Bare integers, swapped argument types, and substituting write frames for read
frames (or vice versa) fail compilation. This intentionally replaces the old
positional-integer construction; there is no compatibility overload.

The stored numeric fields are unchanged: the helpers map to `concurrency`,
`frames_per_write`, `frames_per_read`, and `overlap_frames`, respectively. These counts refer
to pipe slots/frames, not elements within a frame. Runtime validation still
requires positive concurrency and frame counts and
`overlap_frames < frames_per_read`.
In particular, one frame per read requires overlap 0, not 1.

Defaults are unchanged. Default construction or name-only configuration uses
concurrency 1, one frame per write/read, and overlap 0. Trailing arguments may
be omitted, but any explicitly supplied numeric argument must have the correct
type:

```cpp
// Alternatives for configuring a fresh pipe:
numbers.set_producer({"source"}, 0);
numbers.set_consumer({"sink"});
// Or specify only concurrency; frame counts and overlap retain their defaults:
numbers.set_producer({"source", takt::concurrency(2)}, 0);
numbers.set_consumer({"sink", takt::concurrency(3)});
// Or omit just overlap:
numbers.set_consumer({"sink", takt::concurrency(3), takt::frames_per_read(5)});
```

These are alternatives, not repeated calls: each endpoint can be configured only once.
Read acquisition always uses the configured consumer window.

## Slot Capacity and Write Size

Without `set_slot_capacity()`, slot capacity is `max(W, R)`, where
`W = producer.concurrency * producer.frames_per_write` and
`R = consumer.concurrency * (consumer.frames_per_read - consumer.overlap_frames)
+ consumer.overlap_frames`. `request_max_concurrency()` changes this to `W + R`;
repeated requests cannot undo a previous request. These calculations reject
overflow. Concurrency declares capacity demand, not runtime thread permits.

An explicit positive capacity overrides both policies, even when smaller than
either side's concurrency or configured write/read size. The explicit path does
not calculate unused concurrency demand. Acquiring more slots than capacity
throws instead of waiting forever; initialization itself accepts that capacity.

`ProducerConfig::frames_per_write` is both the default token size and maximum explicit
token size:

```cpp
// Reserves frames_per_write slots and does not end the Shot:
auto writes = numbers.acquire_write();
// Reserves 1..frames_per_write slots and explicitly ends the Shot:
auto final_writes = numbers.acquire_shot_end_write(2);
```

Use `acquire_shot_end_write(count)` for a final full or partial token. A
no-argument write is never a ShotEnd, including when it reserves one slot.

Read acquisition has one entry point and uses `ConsumerConfig::frames_per_read` and
`overlap_frames`:

```cpp
auto reads = numbers.acquire_read();
for (size_t i = 0; i < reads.frame_count(); ++i)
    consume(reads.value(i));
```

Each read advances by `frames_per_read - overlap_frames`. During an open Shot, a window
becomes readable as soon as its full stride of newly published slots is
available; it does not wait for ShotEnd. A committed ShotEnd bounds the window
and may return a shorter final window. Overlap values are retained in the ring
buffer; they are not copied into a separate history snapshot. The first window
in each Shot has no overlap, and a ShotEnd resets overlap so the next
Shot never includes frames from the previous Shot. Active read tokens pin
their slots until destruction, so tokens may be processed concurrently after
being acquired in order. The configured overlap also determines the capacity
retained behind the read cursor.

After an unfinished Shot is closed, its published slots remain readable. The
final partial read window has `is_shot_end() == false`; once drained, readers
receive the usual `PipeClosedAndEmptySignal`.

## Runtime Preparation

`PipelineBuilder::build_runtime()` collects pipes from bound port endpoints,
including nested subgraphs. It does not allocate frames or start threads:

```cpp
auto runtime = builder.build_runtime();
runtime->initialize();
runtime->start();
runtime->join();
```

For a directly constructed runtime, pass pipes explicitly when nodes do not
expose bound endpoints: `PipelineRuntime runtime(nodes, {pipe});`.
For stack-owned standalone pipes, initialize them directly before initializing
the runtime. Construct `SubgraphNode` before initializing its internal runtime:
its constructor registers all mapped boundary pipes, including Direct pairs,
with that runtime. Shared pipes are initialized once by object identity, while
different-Pipe Bridge mappings retain both queues. Configure Direct boundaries
with the actual compute producer/consumer, not additional bridge roles.
For a directly started
subgraph, initialize the internal runtime after construction and before start.
Initialize before prefilling and closing input pipes;
runtime preparation rejects closed or faulted connections.

`runtime->request_stop()` closes registered pipes and stops nested runtimes.
It is terminal even before preparation: uninitialized pipes are cancelled and
the runtime cannot subsequently initialize or start. Repeated stop/join is
safe for cleanup. Register pipes explicitly in direct runtimes when nodes do not
expose endpoint descriptors, so runtime shutdown can unblock their acquisitions.

## Runnable Example

- Source: `examples/pipe/main.cpp`
- Build target: `takt_pipe_example`

Run after build:

```bash
./build/examples/pipe/takt_pipe_example
```
