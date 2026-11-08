// Wraps a nested runtime and its direct or bridged boundary connections.
#pragma once

#include <atomic>
#include <future>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "takt/pipeline/channel.h"
#include "takt/pipeline/node.h"
#include "takt/pipeline/pipe_endpoint.h"

namespace takt
{
class PipelineRuntime;

class SubgraphNode final : public NodeBase
{
  public:
    struct MappingLink
    {
        std::shared_ptr<AnyPipeEndpoint> source;
        std::shared_ptr<AnyPipeEndpoint> target;
        std::string source_label;
        std::string target_label;
    };
    // Legacy name: the constructor accepts every mapping, including Direct
    // pairs.
    using BridgeLink = MappingLink;

    // Owns a nested runtime and classifies its bound boundary transports.
    SubgraphNode(std::string node_name,
                 std::shared_ptr<PipelineRuntime> runtime,
                 std::unordered_map<std::string,
                                    NodePortRef> input_port_mappings = {},
                 std::unordered_map<std::string,
                                    NodePortRef> output_port_mappings = {},
                 std::vector<BridgeLink> bridge_links = {});

    // Starts the nested runtime and workers only for actual bridge mappings.
    void start() override;
    // Joins boundary workers and the nested runtime, propagating failures.
    void join() override;
    // Cancels bridges and requests stop throughout the nested runtime.
    void request_stop() noexcept override;
    // Exposes the owned runtime for complete lifecycle-tree traversal.
    std::vector<std::shared_ptr<PipelineRuntime>>
    child_runtimes() const override;

    // Returns logical parent-input to child-input mappings.
    const std::unordered_map<std::string,
                             NodePortRef>&
    input_port_mappings() const noexcept;
    // Returns logical child-output to parent-output mappings.
    const std::unordered_map<std::string,
                             NodePortRef>&
    output_port_mappings() const noexcept;

    // Counts all boundary mappings, including zero-worker direct mappings.
    size_t mapping_count() const noexcept
    {
        return mapping_links_.size();
    }
    // Counts mappings that require dedicated frame-transfer workers.
    size_t bridge_count() const noexcept
    {
        return bridge_links_.size();
    }
    // Reports how many bridge workers were actually launched.
    size_t launched_bridge_worker_count() const noexcept
    {
        return launched_bridge_workers_.load();
    }

  private:
    // Transfers one bridge until closure, propagating boundary errors.
    void run_bridge(size_t index);

    std::shared_ptr<PipelineRuntime> runtime_;
    std::unordered_map<std::string, NodePortRef> input_port_mappings_;
    std::unordered_map<std::string, NodePortRef> output_port_mappings_;
    std::vector<MappingLink> mapping_links_;
    std::vector<BridgeLink> bridge_links_;
    std::vector<std::future<void>> bridge_workers_;
    std::atomic<bool> bridge_stop_requested_{false};
    std::atomic<size_t> launched_bridge_workers_{0};
    std::mutex bridge_error_mutex_;
    bool has_bridge_error_ = false;
    std::string bridge_error_message_;
};
} // namespace takt
