#pragma once

#include <map>
#include <string>
#include <vector>

#include "takt/pipeline/channel.h"

namespace takt
{
struct DotOptions
{
    bool expand_subgraphs = true;
    bool left_to_right = true;
};

// Value-owned topology only: descriptors and runtime objects are not serialized.
struct PipelineGraph
{
    std::string name;
    std::vector<NodeSpec> nodes;
    std::vector<EdgeSpec> edges;
    std::map<std::string, PipelineGraph> subgraphs;
    std::map<std::string, NodePortRef> input_mappings;
    std::map<std::string, NodePortRef> output_mappings;
};

// Also accepts unbound, descriptive graphs; export does not start any workers.
std::string to_dot(const PipelineGraph& graph, DotOptions options = {});
} // namespace takt
