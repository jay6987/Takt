# Pipe Usage Guide

This page shows the minimal usage pattern for `Pipe<T>`.

## Single Producer and Consumer

```cpp
takt::Pipe<int> numbers("numbers", 8, 0);

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

- Always call `publish()` on write tokens that should become visible.
- Call `close()` when no more data will be written.
- Consumer loops should exit on `PipeClosedAndEmptySignal`.

## Runnable Example

- Source: `examples/pipe/main.cpp`
- Build target: `takt_pipe_example`

Run after build:

```bash
./build/examples/pipe/takt_pipe_example
```
