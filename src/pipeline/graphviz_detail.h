// Declares internal DOT validation and rendering operations.
#pragma once

#include "takt/pipeline/graphviz.h"

#include <iosfwd>

namespace takt::dot_detail
{
/// Resolved node identifier and accumulated transport annotation.
struct Endpoint
{
    std::string id;
    std::string annotation;
};

// Escapes a DOT label.
std::string quoted(const std::string& value);
// Resolves a node name to its index.
size_t node_index(const PipelineGraph& graph,
                  const std::string& name);
// Validates the complete nested topology.
void validate_graph(const PipelineGraph& graph);
// Creates a stable internal identifier.
std::string node_id(const std::string& prefix,
                    size_t index);
// Retrieves a mapping's transport information.
MappingTransportInfo transport_info(const PipelineGraph& inner,
                                    const std::string& port,
                                    bool input);
// Creates a stable subgraph boundary identifier.
std::string boundary_id(const NodeSpec& node,
                        const std::string& id,
                        const std::string& port,
                        bool input);
// Writes a labeled edge.
void write_edge(std::ostream& output,
                const std::string& source,
                const std::string& target,
                const std::string& label,
                bool mapping = false);
// Resolves nested boundary connections to their rendered endpoints.
Endpoint resolve_endpoint(const PipelineGraph& graph,
                          const std::string& prefix,
                          const std::string& node,
                          const std::string& port,
                          bool input,
                          const DotOptions& options);
// Reports whether a boundary should be rendered in this view.
bool show_boundary(const PipelineGraph& graph,
                   const NodeSpec& node,
                   const PipelineGraph& inner,
                   const std::string& port,
                   bool input,
                   const DotOptions& options);
// Writes visible boundary ports.
void write_ports(std::ostream& output,
                 const PipelineGraph& graph,
                 const NodeSpec& node,
                 const PipelineGraph& inner,
                 const std::string& id,
                 const DotOptions& options);
// Writes boundary transports.
void write_mappings(std::ostream& output,
                    const PipelineGraph& outer,
                    const NodeSpec& node,
                    const PipelineGraph& inner,
                    const std::string& prefix,
                    const std::string& inner_prefix,
                    const DotOptions& options);
// Writes the complete nested graph.
void write_graph(std::ostream& output,
                 const PipelineGraph& graph,
                 const std::string& prefix,
                 const DotOptions& options);
} // namespace takt::dot_detail
