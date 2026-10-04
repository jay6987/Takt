# Takt

Takt is a data-driven pipeline library inspired by takt time in lean manufacturing.

Current implementation status:
- New root-level open-source layout is initialized.
- Public API namespace is `takt`.
- Logger extension point is available via `takt::ILogger` and `takt::set_logger()`.
- Native pipeline headers are available under `include/takt/pipeline/`.
- Official node worker base is `Node`.
- `Pipe<T>` supports fixed-capacity preallocation, blocking backpressure, batch read/write tokens, and overlap-window reads.
- Node execution is task-driven with a built-in ingress manager and worker pool: users implement `build_task(sequence_id)` and `process_task(worker_index, task)`.
- Structured exception context is available through `NodeExceptionContext` and default logs include stage/pipe/worker metadata.

## Documentation

- [Quick Start](docs/quickstart.md)
- [Record and Replay](docs/record-replay-usage.md)
- [Pipe Lifecycle](docs/pipe-lifecycle.md)
- [Pipe Usage](docs/pipe-usage.md)
- [PipeCore State Machine](docs/pipe-state-machine.md)
- [Pipe Write Sequence](docs/pipe-write-sequence.md)
- [Pipe Read Sequence](docs/pipe-read-sequence.md)
- [Node Model](docs/node-model.md)
- [Node Usage](docs/node-usage.md)
- [Subgraph Usage](docs/subgraph-usage.md)
- [Exception Context](docs/error-context.md)

## Community

- [Contributing Guide](CONTRIBUTING.md)
- [Security Policy](SECURITY.md)
- [Code of Conduct](CODE_OF_CONDUCT.md)
- [Changelog](CHANGELOG.md)

## Quick Start (CMake add_subdirectory)

```cmake
add_subdirectory(path/to/Takt)
target_link_libraries(your_target PRIVATE takt::takt)
```

```cpp
#include "takt/takt.h"

int main() {
    takt::info("hello from takt");
    return 0;
}
```

Pipeline headers:

```cpp
#include "takt/pipeline/pipe.h"
#include "takt/pipeline/node.h"
```

## Node Model

- `Node`: unified node type; default worker count is 1.
- Override `build_task(sequence_id)` and `process_task(worker_index, task)` for workloads.
- Set worker count via `Node("name", worker_count)` when parallelism is needed.
- Use `set_stage_name`, `add_input_port_name`, and `add_output_port_name` to annotate exception context.
- Backpressure should be controlled by `Pipe` capacity and token flow.

## Pipe Model

- Single element: `acquire_write(is_segment_end)` / `acquire_read()`.
- Batch element: `acquire_write_batch(count, is_segment_end)` / `acquire_read_batch(read_size, overlap_size)`.
- Write/read tokens expose `is_segment_start()` and `is_segment_end()` metadata.
- Read batches are segment-bounded and never cross committed segment ends.
- `PipeClosedAndEmptySignal` is used for graceful pipeline termination.

## Logger Injection

Implement `takt::ILogger` and inject globally:

```cpp
class MyLogger : public takt::ILogger {
public:
    void display(const std::string& m) override {}
    void display(const std::wstring& m) override {}
    void info(const std::string& m) override {}
    void info(const std::wstring& m) override {}
    void warn(const std::string& m) override {}
    void warn(const std::wstring& m) override {}
    void error(const std::string& m) override {}
    void error(const std::wstring& m) override {}
    void debug(const std::string& m) override {}
    void debug(const std::wstring& m) override {}
    bool debug_enabled() const override { return true; }
};

takt::set_logger(std::make_shared<MyLogger>());
```

## Roadmap

1. Extend test matrix from smoke test to multi-case CTest suite (deadlock, closure propagation, throughput stress, exception fan-out).
2. Add install/export package support for `find_package(Takt)` workflow.
3. Add benchmark suite for throughput and latency profiling across representative workloads.

## License

This project is licensed under the MIT License. See [LICENSE](LICENSE).
