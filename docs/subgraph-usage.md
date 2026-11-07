# Subgraph Usage Guide

`PipelineBuilder::bind_subgraph` lets you compose an inner pipeline as a node in an outer graph.

## Direct mappings and bridges

Mapped `TypedPipeEndpoint<T>` wrappers sharing the **same Pipe object** use
Direct mappings: the outer and inner compute nodes operate on that pipe without
a bridge worker, second queue, token transfer or buffer swap. Endpoint wrappers
may be different objects. Equal pipe names do not imply shared identity.
Configure the pipe once with its real producer and consumer before initialization.
The builder never replaces pipes already held by nodes.

Different pipes retain Bridge mappings. Compatible typed endpoints transfer
using `transfer_to`: the bridge retains
both tokens and swaps basket storage. For `std::vector<float>`/`double`, this
exchanges preallocated buffers without copying image pixels through `std::any`.
Buffer identity can move between pipes; do not cache a basket's vector data
pointer beyond its token lifetime. Use equal fixed image shapes on both sides,
and disable read-history snapshots for non-overlap image streams.

Explicit endpoint `read()`/`write()` still provide the legacy copied packet API.
Custom endpoint implementations must implement `close_with_error(message)`.
They default to Bridge, preserving custom read/write processing. To opt into
Direct, both endpoints must affirm `supports_direct_to(other)` and share a
non-null `lifecycle_pipe()`. Only opt in if bypassing all endpoint operations is
semantically equivalent. Unsupported same-pipe mappings fail during binding or
construction; explicit `transfer_to` still rejects a pipe bridged to itself.
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

For Direct, use `inner_in = outer_in` and `inner_out = outer_out` when wiring
nodes, and omit bridge producer/consumer configurations. For Bridge, use distinct
pipes and configure bridge participants on their respective sides.
`takt_subgraph_example` demonstrates Direct by default; passing `bridge`
demonstrates the original independent-queue topology.

Create each pipe with a name-only constructor and configure both ends before
preparation (see [Quick Start](quickstart.md)). Count only actual bridges as
participants when declaring each pipe's concurrency demand. Binding a subgraph
retains the same endpoint pipe objects; runtime initialization traverses nested
runtimes and deduplicates shared connections by object identity, not name.
It does not swap pipes during preparation. Do not close or fault an input before
runtime initialization. If starting `SubgraphNode` directly, construct it first
so all mappings register their pipes, then initialize its internal
`PipelineRuntime` before calling `start()`.

The constructor accepts every endpoint pair, including Direct pairs, as
`SubgraphNode::MappingLink` (`BridgeLink` remains a source-compatible alias).
`mapping_count()` counts all pairs, `bridge_count()` counts only transfer workers,
and `launched_bridge_worker_count()` reports successful launches at the latest
start (retained after join). These are local counts, not recursive thread totals.
Lifecycle registration and stop cover all mappings, even when bridge count is
zero. Direct EOF/error propagation belongs to real pipe-owning compute nodes;
there is no bridge to manufacture closure. See [Node Model](node-model.md).

Direct removes queue capacity and changes backpressure/possible overlap.
Retain distinct pipes when independent buffering is intentional. Direct does not
permit multiple producers or consumers to configure one pipe.

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
std::string interfaces = outer.to_dot({true, true, takt::DotView::Interfaces});
```

`DotOptions` controls `expand_subgraphs` (default true), `left_to_right`
(default true), and `view` (default `DotView::Connections`). Existing two-bool
initializers keep their expansion and direction meanings. Write the returned
string to a file using your application's
normal checked I/O. Graphviz is not a Takt build/runtime dependency. If available:

```sh
dot -Tsvg pipeline.dot -o pipeline.svg
```

Expanded subgraphs retain nested `cluster` groups. The default Connections view
resolves Direct mappings recursively to compute nodes, including cross-cluster
edges labeled `direct/shared Pipe`. Bridge mappings show explicit component
nodes labeled `bridge`, `dedicated worker` and both pipe names. Each bridge
has source/target pipe edges; it is not hidden in a dashed mapping.

The Interfaces view retains separate input/output ellipses. Direct mappings are
labeled solid links, Bridge mappings still have explicit transfer nodes, and
unknown mappings are dashed and labeled `unresolved`. Unconnected Direct ports
remain visible in Connections view. Collapsed subgraphs use a 3D box with local
Direct/Bridge/Unresolved mapping counts, not nested worker totals. Normal edges
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

`PipelineGraph::input_transports` / `output_transports` store value-owned
`MappingTransportInfo` keyed by mapped port, with mode and source/target pipe
names. Builder binding populates them using the same classifier as execution.
Hand-built graphs without this metadata are Unresolved, not presumed Bridge.
Explicit metadata on a descriptive graph is a supplied declaration, not observed
worker activity. Orphan metadata, invalid modes and invalid port references
are rejected. Existing mapped aliases retain distinct structural IDs; equal pipe
names do not collapse workers or nodes. Bound snapshots do not serialize Pipe
object identities.

## Runnable Example

- Source: `examples/subgraph/main.cpp`
- Build target: `takt_subgraph_example`

Run after build:

```bash
./build/examples/subgraph/takt_subgraph_example
./build/examples/subgraph/takt_subgraph_example bridge
```

The first command uses shared Direct boundaries; the second uses separate pipes
and two bridge workers. Both produce `2 4 6 8`. The executable location can differ
when Takt is built inside a parent project; use that build's target output path.
