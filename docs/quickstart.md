# Quick Start

## Build As Subdirectory

In your parent CMake project:

```cmake
add_subdirectory(path/to/Takt)
target_link_libraries(your_target PRIVATE takt::takt)
```

In your code:

```cpp
#include "takt/takt.h"

int main() {
    takt::info("hello from takt");
    return 0;
}
```

## Build This Repository

```bash
cmake -S . -B build -DTAKT_BUILD_EXAMPLES=ON -DTAKT_BUILD_TESTS=ON
cmake --build build -j4
ctest --test-dir build --output-on-failure
```

## Core Model

- Backpressure is controlled by `Pipe` capacity and token lifecycle.
- `Node` is a task-driven abstraction with one ingress manager thread and
    configurable worker threads.
- Graceful termination relies on `PipeClosedAndEmptySignal`.

## Subgraph Mapping (Runnable Pattern)

```cpp
#include "takt/takt.h"

int main() {
    auto outer_in = std::make_shared<takt::Pipe<int>>("outer-in", 16, 0);
    auto outer_out = std::make_shared<takt::Pipe<int>>("outer-out", 16, 0);
    auto inner_in = std::make_shared<takt::Pipe<int>>("inner-in", 16, 0);
    auto inner_out = std::make_shared<takt::Pipe<int>>("inner-out", 16, 0);

    auto inner = takt::PipelineBuilder::declare("inner");
    inner.add_node("worker");
    inner.add_input_port(
        "worker",
        "in",
        {{"pipe_endpoint", takt::make_pipe_endpoint<int>(inner_in)}});
    inner.add_output_port(
        "worker",
        "out",
        {{"pipe_endpoint", takt::make_pipe_endpoint<int>(inner_out)}});

    // Bind your own Node implementation here.
    // inner.bind_node("worker", std::make_shared<MyWorkerNode>(inner_in, inner_out));

    auto outer = takt::PipelineBuilder::declare("outer");
    outer.add_node("sg");
    outer.add_input_port(
        "sg",
        "raw",
        {{"pipe_endpoint", takt::make_pipe_endpoint<int>(outer_in)}});
    outer.add_output_port(
        "sg",
        "result",
        {{"pipe_endpoint", takt::make_pipe_endpoint<int>(outer_out)}});

    outer.bind_subgraph(
        "sg",
        inner,
        {{"raw", {"worker", "in"}}},
        {{"result", {"worker", "out"}}});

    auto runtime = outer.build_runtime();
    runtime->start();
    runtime->request_stop();
    runtime->join();
    return 0;
}
```

## Troubleshooting

- `missing pipe_endpoint descriptor ...`:
    - Cause: mapped port is missing `{"pipe_endpoint", make_pipe_endpoint<T>(...)}` in descriptor.
    - Fix: ensure both outer mapped ports and inner mapped ports provide endpoint descriptors.

- `pipe endpoint type mismatch ... expected=... actual=...`:
    - Cause: mapped source and target endpoints use different payload types.
    - Fix: align `Pipe<T>` types on both sides of the mapping.

- `subgraph bridge failure source=... target=...`:
    - Cause: bridge read/write failed (type mismatch, closed endpoint, or other runtime error).
    - Fix: check mapping correctness, endpoint descriptors, and close-order semantics.
