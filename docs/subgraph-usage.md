# Subgraph Usage Guide

`PipelineBuilder::bind_subgraph` lets you compose an inner pipeline as a node in an outer graph.

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
