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
    struct BridgeLink
    {
        std::shared_ptr<AnyPipeEndpoint> source;
        std::shared_ptr<AnyPipeEndpoint> target;
        std::string source_label;
        std::string target_label;
    };

    SubgraphNode(std::string node_name, std::shared_ptr<PipelineRuntime> runtime,
                 std::unordered_map<std::string, NodePortRef> input_port_mappings = {},
                 std::unordered_map<std::string, NodePortRef> output_port_mappings = {},
                 std::vector<BridgeLink> bridge_links = {});

    void start() override;
    void join() override;
    void request_stop() noexcept override;

    const std::unordered_map<std::string, NodePortRef>&
    input_port_mappings() const noexcept;
    const std::unordered_map<std::string, NodePortRef>&
    output_port_mappings() const noexcept;

  private:
    void run_bridge(size_t index);

    std::shared_ptr<PipelineRuntime> runtime_;
    std::unordered_map<std::string, NodePortRef> input_port_mappings_;
    std::unordered_map<std::string, NodePortRef> output_port_mappings_;
    std::vector<BridgeLink> bridge_links_;
    std::vector<std::future<void>> bridge_workers_;
    std::atomic<bool> bridge_stop_requested_{false};
    std::mutex bridge_error_mutex_;
    bool has_bridge_error_ = false;
    std::string bridge_error_message_;
};
} // namespace takt
