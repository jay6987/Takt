# Record and Replay Guide

`recordreplay` lets a `Pipe<T>` record emitted slots to disk and replay slots back into baskets.

## Trivially Copyable Types

For trivially copyable payloads (for example `int`), no codec registration is needed.

```cpp
takt::Pipe<int> record_pipe("record-int");
record_pipe.set_producer({"source", takt::concurrency(1), takt::frames_per_write(3)}, 0);
record_pipe.set_consumer(
    {"sink", takt::concurrency(1), takt::frames_per_read(3), takt::overlap(0)});
record_pipe.set_capacity(8);
record_pipe.initialize();
auto record = takt::recordreplay::make_scoped_record(record_pipe, file_path);

auto w = record_pipe.acquire_shot_end_write(3);
w.value(0) = 101;
w.value(1) = 202;
w.value(2) = 303;
w.publish();
```

Replay:

```cpp
takt::Pipe<int> replay_pipe("replay-int");
replay_pipe.set_producer({"seed", takt::concurrency(1), takt::frames_per_write(3)}, 0);
replay_pipe.set_consumer(
    {"replay", takt::concurrency(1), takt::frames_per_read(3), takt::overlap(0)});
replay_pipe.set_capacity(8);
replay_pipe.initialize();
auto replay = takt::recordreplay::make_scoped_replay(replay_pipe, file_path);

{
    auto seed = replay_pipe.acquire_shot_end_write(3);
    seed.value(0) = -1;
    seed.value(1) = -1;
    seed.value(2) = -1;
    seed.publish();
}

auto r = replay_pipe.acquire_read();
```

The scoped guard must outlive the record/replay phase, and the pipe must outlive
the guard. Destruction detaches the stream without throwing. Call `reset()` when
you need to observe flush or close failures; an error is reported as an exception.
Do not overlap multiple record guards or multiple replay guards for the same
pipe; each direction has a single registry slot, and a second binding is rejected.

## Non-trivial Types

Register a codec first:

```cpp
takt::recordreplay::register_codec<MyType>({
    .record_one = [](std::ostream& os, const MyType& v) { /* ... */ },
    .replay_one = [](std::istream& is, MyType& v) { /* ... */ },
});
```

## Runnable Example

- Source: `examples/recordreplay/main.cpp`
- Build target: `takt_recordreplay_example`

Run after build:

```bash
./build/examples/recordreplay/takt_recordreplay_example
```

## Internal Implementation Map

If you want to see where `Pipe<int>` record/replay behavior is implemented:

1. Public API entry points (`make_scoped_record`, `make_scoped_replay`, codec registration and resolution):
    - `include/takt/recordreplay/recordreplay.h`
2. Stream/file and registry implementation:
    - `src/recordreplay/recordreplay.cpp`
3. Where record is triggered in pipe write lifecycle:
    - `include/takt/pipeline/pipe.h` in `WriteBatchToken::release()`
4. Where replay is triggered in pipe read lifecycle:
    - `include/takt/pipeline/pipe.h` in `Pipe<T>::acquire_read()`
