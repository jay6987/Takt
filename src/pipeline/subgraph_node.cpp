// Executes nested runtimes and transfers frames across bridged boundaries.
#include "takt/pipeline/subgraph_node.h"

#include <exception>
#include <future>
#include <stdexcept>
#include <utility>

#include "takt/logging/logger.h"
#include "takt/pipeline/pipeline_runtime.h"

namespace takt
{
SubgraphNode::SubgraphNode(std::string node_name,
                           std::shared_ptr<PipelineRuntime> runtime,
                           std::unordered_map<std::string,
                                              NodePortRef> input_port_mappings,
                           std::unordered_map<std::string,
                                              NodePortRef> output_port_mappings,
                           std::vector<BridgeLink> bridge_links)
    : NodeBase(std::move(node_name)),
      runtime_(std::move(runtime)),
      input_port_mappings_(std::move(input_port_mappings)),
      output_port_mappings_(std::move(output_port_mappings)),
      mapping_links_(std::move(bridge_links))
{
    if (!runtime_)
        throw std::invalid_argument("subgraph runtime cannot be null");
    configure_mappings();
}

std::vector<std::shared_ptr<PipelineRuntime>>
SubgraphNode::child_runtimes() const
{
    return {runtime_};
}

void SubgraphNode::start()
{
    if (!runtime_->initialized())
        throw std::logic_error(
            "subgraph runtime must be initialized before start");
    bridge_stop_requested_.store(false);
    {
        std::lock_guard<std::mutex> lk(bridge_error_mutex_);
        has_bridge_error_ = false;
        bridge_error_message_.clear();
    }
    bridge_workers_.clear();
    launched_bridge_workers_.store(0);
    bridge_workers_.reserve(bridge_links_.size());
    try
    {
        for (size_t i = 0; i < bridge_links_.size(); ++i)
        {
            bridge_workers_.emplace_back(
                std::async(std::launch::async, [this, i] { run_bridge(i); }));
            launched_bridge_workers_.fetch_add(1);
        }
        runtime_->start();
    }
    catch (...)
    {
        handle_start_failure(std::current_exception());
    }
}

void SubgraphNode::handle_start_failure(std::exception_ptr failure)
{
    request_stop();
    try
    {
        join();
    }
    catch (const std::exception& cleanup)
    {
        error(std::string("subgraph startup cleanup failed: ") +
              cleanup.what());
    }
    catch (...)
    {
        error("subgraph startup cleanup failed: unknown exception");
    }
    std::rethrow_exception(failure);
}

void SubgraphNode::join()
{
    std::exception_ptr failure;
    try
    {
        runtime_->join();
    }
    catch (...)
    {
        failure = std::current_exception();
    }

    for (auto& worker : bridge_workers_)
    {
        if (worker.valid())
        {
            try
            {
                worker.get();
            }
            catch (...)
            {
                if (!failure)
                {
                    failure = std::current_exception();
                }
            }
        }
    }
    bridge_workers_.clear();

    if (failure)
    {
        std::rethrow_exception(failure);
    }
    std::lock_guard<std::mutex> lk(bridge_error_mutex_);
    if (has_bridge_error_)
    {
        throw std::runtime_error(bridge_error_message_);
    }
}

const std::unordered_map<std::string,
                         NodePortRef>&
SubgraphNode::input_port_mappings() const noexcept
{
    return input_port_mappings_;
}

const std::unordered_map<std::string,
                         NodePortRef>&
SubgraphNode::output_port_mappings() const noexcept
{
    return output_port_mappings_;
}

} // namespace takt
