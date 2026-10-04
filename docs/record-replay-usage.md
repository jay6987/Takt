# Record and Replay Guide

`recordreplay` lets a `Pipe<T>` record emitted slots to disk and replay slots back into baskets.

## Trivially Copyable Types

For trivially copyable payloads (for example `int`), no codec registration is needed.

```cpp
takt::Pipe<int> record_pipe("record-int", 8, 0);
takt::recordreplay::set_record(record_pipe, file_path);

auto w = record_pipe.acquire_write_batch(3, true);
w.value(0) = 101;
w.value(1) = 202;
w.value(2) = 303;
w.publish();
```

Replay:

```cpp
takt::Pipe<int> replay_pipe("replay-int", 8, 0);
takt::recordreplay::set_replay(replay_pipe, file_path);

{
    auto seed = replay_pipe.acquire_write_batch(3, true);
    seed.value(0) = -1;
    seed.value(1) = -1;
    seed.value(2) = -1;
    seed.publish();
}

auto r = replay_pipe.acquire_read_batch(3, 0);
```

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

1. Public API entry points (`set_record`, `set_replay`, codec resolution):
    - `include/takt/recordreplay/recordreplay.h`
2. Stream/file and registry implementation:
    - `src/recordreplay/recordreplay.cpp`
3. Where record is triggered in pipe write lifecycle:
    - `include/takt/pipeline/pipe.h` in `WriteBatchToken::release()`
4. Where replay is triggered in pipe read lifecycle:
    - `include/takt/pipeline/pipe.h` in `Pipe<T>::acquire_read_batch()`
