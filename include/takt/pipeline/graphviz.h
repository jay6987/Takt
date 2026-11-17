// Exposes DOT views of bound pipelines without a Graphviz runtime dependency.
#pragma once

#include <map>
#include <string>
#include <vector>

#include "takt/pipeline/channel.h"

namespace takt
{
enum class DotView
{
    Connections,
    Interfaces,
};

/// Controls the orientation, expansion and presentation of DOT output.
struct DotOptions
{
    bool expand_subgraphs = true;
    bool left_to_right = true;
    DotView view = DotView::Connections;
};

/// Value-owned topology; descriptors and runtime objects are not serialized.
struct PipelineGraph
{
    std::string name;
    std::vector<NodeSpec> nodes;
    std::vector<EdgeSpec> edges;
    std::map<std::string, PipelineGraph> subgraphs;
    std::map<std::string, NodePortRef> input_mappings;
    std::map<std::string, NodePortRef> output_mappings;
    std::map<std::string, MappingTransportInfo> input_transports;
    std::map<std::string, MappingTransportInfo> output_transports;
};

// Also accepts unbound, descriptive graphs; export does not start any workers.
std::string to_dot(const PipelineGraph& graph,
                   DotOptions options = {});
} // namespace takt
