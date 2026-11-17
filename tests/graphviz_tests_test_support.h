// Verifies DOT escaping, topology views and bound subgraph transport labels.
#pragma once
#include "takt/takt.h"

#include <gtest/gtest.h>

#include <memory>
#include <stdexcept>
#include <string>

namespace
{
// Returns a minimal valid source-to-sink graph for DOT variations.
inline takt::PipelineGraph chain()
{
    takt::PipelineGraph graph;
    graph.name = "test";
    graph.nodes = {
        {"source", {}, {}, {{"out", takt::PortDirection::Output, {}}}},
        {"sink", {}, {{"in", takt::PortDirection::Input, {}}}, {}}};
    graph.edges = {{"frames", "source", "out", "sink", "in", {}}};
    return graph;
}

/// Builds a nested topology with independently selectable boundary transports.
inline takt::PipelineGraph mapped_graph(takt::MappingTransport input,
                                        takt::MappingTransport output)
{
    takt::PipelineGraph inner;
    inner.name = "inner";
    inner.nodes = {{"worker",
                    {},
                    {{"in", takt::PortDirection::Input, {}}},
                    {{"out", takt::PortDirection::Output, {}}}}};
    inner.input_mappings = {{"in", {"worker", "in"}}};
    inner.output_mappings = {{"out", {"worker", "out"}}};
    inner.input_transports = {
        {"in",
         {input, "input",
          input == takt::MappingTransport::Direct ? "input" : "inner-input"}}};
    inner.output_transports = {
        {"out",
         {output,
          output == takt::MappingTransport::Direct ? "output" : "inner-output",
          "output"}}};
    takt::PipelineGraph graph;
    graph.name = "mapped";
    graph.nodes = {
        {"source", {}, {}, {{"out", takt::PortDirection::Output, {}}}},
        {"group",
         {},
         {{"in", takt::PortDirection::Input, {}}},
         {{"out", takt::PortDirection::Output, {}}}},
        {"sink", {}, {{"in", takt::PortDirection::Input, {}}}, {}}};
    graph.edges = {{"input", "source", "out", "group", "in", {}},
                   {"output", "group", "out", "sink", "in", {}}};
    graph.subgraphs = {{"group", inner}};
    return graph;
}

/// Represents the idle node test fixture.
class IdleNode final : public takt::Node
{
  public:
    // Creates an inert executable binding for graph-export tests.
    IdleNode() : Node("idle") {}

  private:
    // Ends ingress immediately without producing processing work.
    std::unique_ptr<Task> build_task(uint64_t) override
    {
        return nullptr;
    }
    // Performs no processing because the fixture never creates tasks.
    void process_task(size_t,
                      Task&) override
    {
    }
};
} // namespace
