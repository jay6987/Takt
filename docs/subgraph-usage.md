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
```

## Required Descriptor

For mapped ports, set `pipe_endpoint` on both sides:

- Outer mapped input/output ports.
- Inner mapped input/output ports.

Missing this descriptor causes `missing pipe_endpoint descriptor ...` errors.

## Runnable Example

- Source: `examples/subgraph/main.cpp`
- Build target: `takt_subgraph_example`

Run after build:

```bash
./build/examples/subgraph/takt_subgraph_example
```
