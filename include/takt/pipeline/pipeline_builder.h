#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "takt/pipeline/channel.h"

namespace takt
{
class NodeBase;
class PipelineRuntime;

class PipelineBuilder
{
  public:
    static PipelineBuilder declare(std::string name);

    PipelineBuilder(const PipelineBuilder&);
    PipelineBuilder& operator=(const PipelineBuilder&);
    PipelineBuilder(PipelineBuilder&&) noexcept;
    PipelineBuilder& operator=(PipelineBuilder&&) noexcept;
    ~PipelineBuilder();

    const std::string& name() const noexcept;

    PipelineBuilder& add_node(std::string node_name, PipeDescriptor descriptor = {});
    PipelineBuilder& add_input_port(std::string node_name, std::string port_name,
                                    PipeDescriptor descriptor = {});
    PipelineBuilder& add_output_port(std::string node_name, std::string port_name,
                                     PipeDescriptor descriptor = {});
    PipelineBuilder& connect(std::string from_node, std::string from_port,
                             std::string to_node, std::string to_port,
                             PipeDescriptor descriptor = {},
                             std::string channel_name = {});
    PipelineBuilder& bind_node(std::string node_name, std::shared_ptr<NodeBase> node);
    PipelineBuilder& bind_subgraph(std::string node_name,
                                   const PipelineBuilder& subgraph_builder);
    PipelineBuilder&
    bind_subgraph(std::string node_name, const PipelineBuilder& subgraph_builder,
                  std::unordered_map<std::string, NodePortRef> input_port_mappings,
                  std::unordered_map<std::string, NodePortRef> output_port_mappings);

    bool has_node(const std::string& node_name) const;
    bool has_bound_node(const std::string& node_name) const;
    const NodeSpec* find_node(const std::string& node_name) const;
    const EdgeSpec* find_edge(const std::string& edge_name) const;
    const std::vector<NodeSpec>& nodes() const noexcept;
    const std::vector<EdgeSpec>& edges() const noexcept;
    std::shared_ptr<NodeBase> find_bound_node(const std::string& node_name) const;

    std::shared_ptr<PipelineRuntime> build_runtime() const;

    void validate() const;

    struct State;

  private:
    explicit PipelineBuilder(std::shared_ptr<State> state);

    std::shared_ptr<State> state_;
};
} // namespace takt