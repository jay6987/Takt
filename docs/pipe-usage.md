# Pipe Usage Guide

This page shows the minimal usage pattern for `Pipe<T>`.

## Single Producer and Consumer

```cpp
takt::Pipe<int> numbers("numbers");
numbers.set_producer({"source", takt::concurrency(1), takt::frames_per_write(1)}, 0);
numbers.set_consumer(
    {"sink", takt::concurrency(1), takt::frames_per_read(1), takt::overlap(0)});
numbers.set_capacity(8);
numbers.initialize();

for (int i = 1; i <= 5; ++i)
{
    auto w = numbers.acquire_write(true);
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
- Configure both ends and the producer's typed basket template before
  `initialize()`. Either end may configure first; configuration is coordinated
  serially, not concurrently with initialization or workers.
- `ProducerConfig` fields are `name`, `concurrency`, `write_size`;
  `ConsumerConfig` fields are `name`, `concurrency`, `read_size`, `overlap_size`.
- Optional `set_descriptor()` and `set_read_history_policy()` also belong to
  configuration. After initialization, all configuration is frozen.
- A standalone pipe must be explicitly initialized before any token acquisition.
- Always call `publish()` on write tokens that should become visible.
- Call `close()` when no more data will be written.
- Consumer loops should exit on `PipeClosedAndEmptySignal`.

## Named Configuration Arguments

Configuration constructors require distinct strong types, in this fixed order:

```cpp
// Producer: name, concurrency, frames per write
numbers.set_producer({"source", takt::concurrency(2), takt::frames_per_write(4)}, 0);
// Consumer: name, concurrency, frames per read, overlap
numbers.set_consumer(
    {"sink", takt::concurrency(3), takt::frames_per_read(5), takt::overlap(2)});
```

`concurrency()` returns `Concurrency`, `frames_per_write()` returns
`FramesPerWrite`, `frames_per_read()` returns `FramesPerRead`, and `overlap()`
returns `Overlap`. Each wraps a `size_t` without implicit conversions.
Bare integers, swapped argument types, and substituting write frames for read
frames (or vice versa) fail compilation. This intentionally replaces the old
positional-integer construction; there is no compatibility overload.

The stored numeric fields are unchanged: the helpers map to `concurrency`,
`write_size`, `read_size`, and `overlap_size`, respectively. These counts refer
to pipe slots/baskets, not elements within a basket. Runtime validation still
requires positive concurrency and frame counts and `overlap < frames_per_read`.
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
The per-call acquisition APIs below still accept ordinary integer counts.

## Capacity and Batch Defaults

Without `set_capacity()`, capacity is `max(W, R)`, where
`W = producer.concurrency * producer.write_size` and
`R = consumer.concurrency * (consumer.read_size - consumer.overlap_size)
+ consumer.overlap_size`. `request_max_concurrency()` changes this to `W + R`;
repeated requests cannot undo a previous request. These calculations reject
overflow. Concurrency declares capacity demand, not runtime thread permits.

An explicit positive capacity overrides both policies, even when smaller than
either side's concurrency or configured batch. The explicit path does not
calculate unused concurrency demand. Acquiring an impossible batch throws
instead of waiting forever; initialization itself accepts that capacity.

The acquisition APIs deliberately remain distinct:

```cpp
// Always one element, independent of configured batch defaults:
auto scalar = numbers.acquire_write(true);
// Defaults from ProducerConfig / ConsumerConfig; returns batch tokens:
auto writes = numbers.acquire_configured_write_batch(true);
auto reads = numbers.acquire_configured_read_batch();
// Per-call override, still returns batch tokens:
auto explicit_writes = numbers.acquire_write_batch(2, true);
auto explicit_reads = numbers.acquire_read_batch(2, 0);
```

These are alternative calls, not a sequence to run while retaining all tokens.
Overlap is composed from completed read history and never crosses a segment.
When configured overlap is nonzero, bootstrap history with an explicit
`acquire_read_batch(read_size, 0)` before using configured reads.
`ReadHistoryPolicy::Disabled` with configured overlap is rejected at initialization.

## Runtime Preparation

`PipelineBuilder::build_runtime()` collects pipes from bound port endpoints,
including nested subgraphs. It does not allocate baskets or start threads:

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
