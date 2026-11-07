# Subgraph Usage Guide

`PipelineBuilder::bind_subgraph` lets you compose an inner pipeline as a node in an outer graph.

Compatible typed endpoints now transfer using `transfer_to`: the bridge retains
both tokens and swaps basket storage. For `std::vector<float>`/`double`, this
exchanges preallocated buffers without copying image pixels through `std::any`.
Buffer identity can move between pipes; do not cache a basket's vector data
pointer beyond its token lifetime. Use equal fixed image shapes on both sides,
and disable read-history snapshots for non-overlap image streams.

Explicit endpoint `read()`/`write()` still provide the legacy copied packet API.
Custom endpoint implementations must implement `close_with_error(message)`.
Bridge errors close both affected endpoints with an error, waking upstream
writers and propagating failure downstream after successful data drains.

## Mapping Pattern

```cpp
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
inner.bind_node("worker", std::make_shared<DoubleNode>(inner_in, inner_out));

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

outer.bind_subgraph("sg", inner,
                    {{"raw", {"worker", "in"}}},
                    {{"result", {"worker", "out"}}});

auto runtime = outer.build_runtime();
runtime->initialize();
runtime->start();
runtime->join();
```

Create each pipe with a name-only constructor and configure both ends before
preparation (see [Quick Start](quickstart.md)). Count the input/output bridges as
participants when declaring each pipe's concurrency demand. Binding a subgraph
retains the same endpoint pipe objects; runtime initialization traverses nested
runtimes and deduplicates shared connections by object identity, not name.
It does not swap pipes during preparation. Do not close or fault an input before
runtime initialization. If starting `SubgraphNode` directly, construct it first
so its bridges register their pipes, then initialize its internal
`PipelineRuntime` before calling `start()`.

## Required Descriptor

For mapped ports, set `pipe_endpoint` on both sides:

- Outer mapped input/output ports.
- Inner mapped input/output ports.

Missing this descriptor causes `missing pipe_endpoint descriptor ...` errors.

## DOT export

Export the declared topology before or after binding nodes; no workers are started:

```cpp
std::string expanded = outer.to_dot();
std::string collapsed = outer.to_dot({false, true});
std::string vertical = outer.to_dot({true, false});
```

`DotOptions` controls `expand_subgraphs` (default true) and `left_to_right`
(default true). Write the returned string to a file using your application's
normal checked I/O. Graphviz is not a Takt build/runtime dependency. If available:

```sh
dot -Tsvg pipeline.dot -o pipeline.svg
```

Expanded subgraphs become nested `cluster` groups with separate input/output
boundary nodes and dashed port-mapping edges. This avoids artificial cycles
through a single interface node. Collapsed subgraphs use a 3D box. Normal edges
show channel names (when provided) and source/target port names.
Names are escaped as DOT strings, and generated structural IDs cannot collide
when names repeat in different subgraphs. Output order is deterministic for the
same topology; arbitrary descriptors, endpoint pointers and payloads are not
exported. Invalid node/port references and unsupported control characters throw.

`builder.graph()` returns a value snapshot. Bound subgraphs retain their topology
at binding time, matching their built runtime; later edits to an inner builder
do not silently change the exported bound subgraph. Rebinding that node with
`bind_node()` removes the old subgraph description. Export snapshots are taken
under the builder's mutex; configure/bind builders before starting workers.

For diagrams that describe an algorithm without a runnable Takt pipeline,
construct `PipelineGraph` directly and call `takt::to_dot(graph)`. Populate
`nodes`/`edges`, attach inner graphs in `subgraphs` keyed by outer node name,
and set each inner graph's `input_mappings`/`output_mappings`. Such a diagram
describes supplied topology only: it is not evidence that a runtime was executed
or a performance trace.

## Runnable Example

- Source: `examples/subgraph/main.cpp`
- Build target: `takt_subgraph_example`

Run after build:

```bash
./build/examples/subgraph/takt_subgraph_example
```
