// Validates and registers subgraph boundary mappings.
#include "takt/pipeline/subgraph_node.h"

#include <stdexcept>

#include "takt/pipeline/pipeline_runtime.h"

namespace takt
{
void SubgraphNode::configure_mappings()
{
    for (const auto& link : mapping_links_)
    {
        if (!link.source || !link.target)
            throw std::invalid_argument(
                "subgraph bridge endpoints cannot be null");
    }

    const size_t expected_mapping_count =
        input_port_mappings_.size() + output_port_mappings_.size();
    if (mapping_links_.size() != expected_mapping_count)
        throw std::logic_error(
            "subgraph mapping link count mismatch: expected=" +
            std::to_string(expected_mapping_count) +
            " actual=" + std::to_string(mapping_links_.size()));
    for (const auto& link : mapping_links_)
    {
        if (classify_mapping(*link.source, *link.target).mode ==
            MappingTransport::Bridge)
            bridge_links_.push_back(link);
    }
    for (const auto& link : mapping_links_)
    {
        runtime_->add_pipe(link.source->lifecycle_pipe());
        runtime_->add_pipe(link.target->lifecycle_pipe());
    }
}
} // namespace takt
