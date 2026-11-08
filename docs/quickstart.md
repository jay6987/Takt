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
- Create and wire name-only pipes, configure both ends, then call
  `PipelineRuntime::initialize()` before `start()`. Standalone pipes call their
  own `initialize()` before acquisition. See [Pipe Usage](pipe-usage.md).

## Upgrading an Installed Package

Use a fresh Takt installation prefix for the lifecycle migration, or remove
only the obsolete `include/takt/pipeline/builder.h` from the previous prefix
before reinstalling. `install(DIRECTORY include/)` does not delete headers that
disappeared from the source tree. Reconfigure consumers against the new prefix;
do not use an old PipeBuilder header from another include directory. The
installed-package smoke consumer rejects the removed header if discoverable.

## Subgraph Mapping (Wiring Template)

Use one shared Pipe per Direct boundary. Only the real producer and consumer
configure it; the subgraph does not add a participant or worker.
Bind your compute node where indicated before creating the outer runtime.
For a complete executable, see [Subgraph Usage](subgraph-usage.md#runnable-example).

```cpp
#include "takt/takt.h"

int main() {
    auto outer_in = std::make_shared<takt::Pipe<int>>("outer-in");
    outer_in->set_producer({"source", takt::concurrency(1), takt::frames_per_write(1)}, 0);
    outer_in->set_consumer(
        {"worker", takt::concurrency(1), takt::frames_per_read(1), takt::overlap_frames(0)});
    auto outer_out = std::make_shared<takt::Pipe<int>>("outer-out");
    outer_out->set_producer({"worker", takt::concurrency(1), takt::frames_per_write(1)}, 0);
    outer_out->set_consumer(
        {"sink", takt::concurrency(1), takt::frames_per_read(1), takt::overlap_frames(0)});
    auto inner_in = outer_in;
    auto inner_out = outer_out;
    for (const auto& pipe : {outer_in, outer_out})
        pipe->set_slot_capacity(16);

    auto inner = takt::PipelineBuilder::create("inner");
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

    auto outer = takt::PipelineBuilder::create("outer");
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
    runtime->initialize();
    runtime->start();
    runtime->request_stop();
    runtime->join();
    return 0;
}
```

The two separately created endpoint wrappers for each boundary still resolve to
Direct because their typed Pipe object is identical. Distinct pipes retain the
Bridge path, even when named identically. If independent buffering is intentional,
use separate inner/outer pipes and configure the bridge's roles on both sides.
Custom endpoints default to Bridge; same-Pipe custom mappings must explicitly
support Direct on both endpoints or binding fails.

DOT export does not start or initialize workers:

```cpp
auto connections = outer.to_dot();
auto interfaces = outer.to_dot({true, true, takt::DotView::Interfaces});
auto collapsed = outer.to_dot({false, true});
```

Connections resolves Direct ports to compute-node connections; Interfaces keeps
logical port ellipses. Bridges are explicit worker nodes in either expanded
view. Descriptive mappings without mode metadata are dashed `unresolved`
connections, not evidence of bridge execution.

## Troubleshooting

- `pipe='...' ... configuration is missing` or `pipe is not initialized`:
    - Configure producer/template and consumer on the same shared pipe object.
    - Initialize the runtime before starting or prefilling/closing input pipes.
- Configuration frozen/cancelled errors:
    - Finish configuration before initialization; cancelled pipes cannot restart.

- `missing pipe_endpoint descriptor ...`:
    - Cause: mapped port is missing `{"pipe_endpoint", make_pipe_endpoint<T>(...)}` in descriptor.
    - Fix: ensure both outer mapped ports and inner mapped ports provide endpoint descriptors.

- `pipe endpoint type mismatch ... expected=... actual=...`:
    - Cause: mapped source and target endpoints use different payload types.
    - Fix: align `Pipe<T>` types on both sides of the mapping.

- `endpoints do not support direct mapping of pipe='...'`:
    - Cause: a same-Pipe pair includes endpoints that cannot safely bypass their operations.
    - Fix: use typed wrappers or explicitly opt both custom endpoints into Direct
      only when bypassing processing preserves semantics. Otherwise use distinct pipes.

- `stop_reason='bridge_exception'`:
    - Cause: bridge read/write failed (type mismatch, closed endpoint, or other runtime error).
    - Fix: check mapping correctness, endpoint descriptors, and close-order semantics.
