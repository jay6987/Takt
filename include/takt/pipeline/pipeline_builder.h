// Builds named nodes, pipes and validated nested runtime connections.
#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "takt/pipeline/channel.h"
#include "takt/pipeline/graphviz.h"

namespace takt
{
class NodeBase;
class PipelineRuntime;

class PipelineBuilder
{
  public:
    // Returns shared named builder state retained in the declaration registry.
    static PipelineBuilder declare(std::string name);
    // Independent lifetime; unlike declare(), does not retain state in the
    // named registry.
    static PipelineBuilder create(std::string name);

    // Shares ownership of the same mutable builder state.
    PipelineBuilder(const PipelineBuilder&);
    // Replaces this handle with shared ownership of another builder state.
    PipelineBuilder& operator=(const PipelineBuilder&);
    // Transfers ownership of the builder handle.
    PipelineBuilder(PipelineBuilder&&) noexcept;
    // Transfers a builder handle, releasing its previous state.
    PipelineBuilder& operator=(PipelineBuilder&&) noexcept;
    // Releases this handle's ownership of shared builder state.
    ~PipelineBuilder();

    // Returns the topology's declared name.
    const std::string& name() const noexcept;

    // Declares a unique node with optional diagnostic metadata.
    PipelineBuilder& add_node(std::string node_name,
                              PipeDescriptor descriptor = {});
    // Declares a named input port on an existing node.
    PipelineBuilder& add_input_port(std::string node_name,
                                    std::string port_name,
                                    PipeDescriptor descriptor = {});
    // Declares a named output port on an existing node.
    PipelineBuilder& add_output_port(std::string node_name,
                                     std::string port_name,
                                     PipeDescriptor descriptor = {});
    // Connects compatible output/input ports with optional channel metadata.
    PipelineBuilder& connect(std::string from_node,
                             std::string from_port,
                             std::string to_node,
                             std::string to_port,
                             PipeDescriptor descriptor = {},
                             std::string channel_name = {});
    // Associates a declared node with its executable runtime object.
    PipelineBuilder& bind_node(std::string node_name,
                               std::shared_ptr<NodeBase> node);
    // Binds a nested topology using its default boundary mappings.
    PipelineBuilder& bind_subgraph(std::string node_name,
                                   const PipelineBuilder& subgraph_builder);
    // Binds a nested topology with explicit input and output mappings.
    PipelineBuilder&
    bind_subgraph(std::string node_name,
                  const PipelineBuilder& subgraph_builder,
                  std::unordered_map<std::string,
                                     NodePortRef> input_port_mappings,
                  std::unordered_map<std::string,
                                     NodePortRef> output_port_mappings);

    // Reports whether a node is declared in this topology.
    bool has_node(const std::string& node_name) const;
    // Reports whether a node has an executable runtime binding.
    bool has_bound_node(const std::string& node_name) const;
    // Returns declared node metadata or null if absent.
    const NodeSpec* find_node(const std::string& node_name) const;
    // Returns declared edge metadata or null if absent.
    const EdgeSpec* find_edge(const std::string& edge_name) const;
    // Returns the declared node sequence without copying it.
    const std::vector<NodeSpec>& nodes() const noexcept;
    // Returns the declared edge sequence without copying it.
    const std::vector<EdgeSpec>& edges() const noexcept;
    // Returns a bound runtime node or null if absent.
    std::shared_ptr<NodeBase>
    find_bound_node(const std::string& node_name) const;

    // Builds a validated executable runtime without starting workers.
    std::shared_ptr<PipelineRuntime> build_runtime() const;
    // Copies topology and mapping information into a value-owned graph.
    PipelineGraph graph() const;
    // Exports topology using the requested DOT view without executing it.
    std::string to_dot(DotOptions options = {}) const;

    // Rejects invalid ports, edges and nested boundary mappings.
    void validate() const;

    struct State;

  private:
    // Creates a handle around existing shared builder state.
    explicit PipelineBuilder(std::shared_ptr<State> state);

    std::shared_ptr<State> state_;
};
} // namespace takt