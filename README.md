# Takt

Takt is a data-driven pipeline library inspired by takt time in lean manufacturing.

Current implementation status:
- C++17 pipeline library with public API namespace `takt`.
- Logger extension point is available via `takt::ILogger` and `takt::set_logger()`.
- Native pipeline headers are available under `include/takt/pipeline/`.
- Official node worker base is `Node`.
- `Pipe<T>` has explicit configuration/initialization, fixed-capacity preallocation,
  blocking backpressure, batch read/write tokens, and overlap-window reads.
- Node execution is task-driven with a built-in ingress manager and worker pool: users implement `build_task(sequence_id)` and `process_task(worker_index, task)`.
- Structured exception context is available through `NodeExceptionContext` and default logs include stage/pipe/worker metadata.
- Subgraphs retain logical grouping without mandatory transfer threads: typed
  endpoints sharing one Pipe use Direct mappings; distinct pipes keep Bridge
  workers. Custom endpoints must explicitly opt into Direct.
- DOT export provides compute-connection and logical-interface views, explicit
  bridge nodes and unresolved descriptive mappings without starting workers.
- Record/replay bindings use move-only scoped guards with checked explicit cleanup.
- CTest covers pipeline behavior, record/replay cleanup, concurrency, and package installation.

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

## Quick Start

### CMake add_subdirectory

```cmake
add_subdirectory(path/to/Takt)
target_link_libraries(your_target PRIVATE takt::takt)
```

### Installed CMake package

Install Takt from a checkout:

```bash
cmake -S path/to/Takt -B build
cmake --build build --parallel
cmake --install build --prefix /path/to/install
```

For this breaking lifecycle upgrade, use a fresh install prefix, or remove only
the obsolete `include/takt/pipeline/builder.h` from your existing Takt prefix
before installing. CMake's directory installation copies current headers but
does not prune removed headers from previous versions. Do not retain the old
header as a compatibility API. The installed-package smoke test uses a clean
prefix and rejects any discoverable `takt/pipeline/builder.h`.

Then find and link the exported target from another CMake project:

```cmake
find_package(Takt 0.2 CONFIG REQUIRED)
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

Use `PipelineBuilder::create(name)` for a scoped, independent graph: its state and
bound nodes are released when the builder copies and runtimes release ownership.
`PipelineBuilder::declare(name)` instead reuses state in the process-wide named
registry. Stop and join running nodes before releasing their dependencies.

- `Node`: unified node type; default worker count is 1.
- Override `build_task(sequence_id)` and `process_task(worker_index, task)` for workloads.
- Set worker count via `Node("name", worker_count)` when parallelism is needed.
- Use `set_stage_name`, `add_input_port_name`, and `add_output_port_name` to annotate exception context.
- Backpressure should be controlled by `Pipe` capacity and token flow.

## Pipe Model

- Construct `Pipe<T>("name")`, configure producer/template and consumer on that
  same object, then initialize. Names are labels and do not share connections.
- `PipelineRuntime::initialize()` prepares bound endpoint pipes and nested
  runtimes; call it before `start()`. Direct runtimes can receive explicit pipes.
- Single element: `acquire_write(is_segment_end)` / `acquire_read()`.
- Batch element: `acquire_write_batch(count, is_segment_end)` / `acquire_read_batch(read_size, overlap_size)`.
- Configured batch: `acquire_configured_write_batch(is_segment_end)` /
  `acquire_configured_read_batch()`. Scalar APIs never adopt batch defaults.
- Write/read tokens expose `is_segment_start()` and `is_segment_end()` metadata.
- Read batches are segment-bounded and never cross committed segment ends.
- `PipeClosedAndEmptySignal` is used for graceful pipeline termination.

### Breaking lifecycle migration

`PipeBuilder` and constructors taking capacity/template are removed, not
deprecated compatibility wrappers. This applies equally to checkout and
installed-package consumers:

```cpp
auto frames = std::make_shared<takt::Pipe<std::vector<float>>>("frames");
frames->set_producer({"source", 1, 1}, std::vector<float>(1024));
frames->set_consumer({"sink", 1, 1, 0});
frames->set_capacity(2); // optional positive override, no concurrency lower bound
frames->set_read_history_policy(takt::ReadHistoryPolicy::Disabled);
frames->initialize(); // standalone; a bound runtime can prepare it instead
```

Without an explicit capacity, the default satisfies the larger endpoint slot
demand; `request_max_concurrency()` requests their sum. Finish configuration
before initialization and initialize before any prefill/close. See
[Pipe Usage](docs/pipe-usage.md) for formulas, configured batches, and runtime
preparation. The library still requires C++17.

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

1. Add benchmark suite for throughput and latency profiling across representative workloads.

## License

This project is licensed under the MIT License. See [LICENSE](LICENSE).
