#include "takt/pipeline/subgraph_node.h"

#include <exception>
#include <future>
#include <stdexcept>
#include <utility>

#include "takt/logging/logger.h"
#include "takt/pipeline/pipeline_runtime.h"

namespace takt
{
SubgraphNode::SubgraphNode(
    std::string node_name, std::shared_ptr<PipelineRuntime> runtime,
    std::unordered_map<std::string, NodePortRef> input_port_mappings,
    std::unordered_map<std::string, NodePortRef> output_port_mappings,
    std::vector<BridgeLink> bridge_links)
    : NodeBase(std::move(node_name)), runtime_(std::move(runtime)),
      input_port_mappings_(std::move(input_port_mappings)),
      output_port_mappings_(std::move(output_port_mappings)),
      bridge_links_(std::move(bridge_links))
{
    if (!runtime_)
    {
        throw std::invalid_argument("subgraph runtime cannot be null");
    }

    for (const auto& link : bridge_links_)
    {
        if (!link.source || !link.target)
        {
            throw std::invalid_argument("subgraph bridge endpoints cannot be null");
        }
    }

    const size_t expected_bridge_count =
        input_port_mappings_.size() + output_port_mappings_.size();
    if (bridge_links_.size() != expected_bridge_count)
    {
        throw std::logic_error("subgraph bridge link count mismatch: expected=" +
                               std::to_string(expected_bridge_count) +
                               " actual=" + std::to_string(bridge_links_.size()));
    }
}

void SubgraphNode::start()
{
    bridge_stop_requested_.store(false);
    {
        std::lock_guard<std::mutex> lk(bridge_error_mutex_);
        has_bridge_error_ = false;
        bridge_error_message_.clear();
    }
    bridge_workers_.clear();
    bridge_workers_.reserve(bridge_links_.size());
    for (size_t i = 0; i < bridge_links_.size(); ++i)
    {
        bridge_workers_.emplace_back(
            std::async(std::launch::async, [this, i] { run_bridge(i); }));
    }

    runtime_->start();
}

void SubgraphNode::join()
{
    runtime_->join();

    for (auto& worker : bridge_workers_)
    {
        if (worker.valid())
        {
            worker.get();
        }
    }
    bridge_workers_.clear();

    std::lock_guard<std::mutex> lk(bridge_error_mutex_);
    if (has_bridge_error_)
    {
        throw std::runtime_error(bridge_error_message_);
    }
}

void SubgraphNode::request_stop() noexcept
{
    NodeBase::request_stop();
    bridge_stop_requested_.store(true);
    for (const auto& link : bridge_links_)
    {
        link.source->close();
        link.target->close();
    }
    runtime_->request_stop();
}

const std::unordered_map<std::string, NodePortRef>&
SubgraphNode::input_port_mappings() const noexcept
{
    return input_port_mappings_;
}

const std::unordered_map<std::string, NodePortRef>&
SubgraphNode::output_port_mappings() const noexcept
{
    return output_port_mappings_;
}

void SubgraphNode::run_bridge(size_t index)
{
    auto& link = bridge_links_[index];
    while (!bridge_stop_requested_.load())
    {
        try
        {
            auto packet = link.source->read();
            link.target->write(packet);
        }
        catch (const PipeClosedAndEmptySignal&)
        {
            link.target->close();
            break;
        }
        catch (const std::exception& e)
        {
            const std::string message =
                "pipeline='" + pipeline_name() + "' node='" + name() + "' edge='" +
                link.source_label + "->" + link.target_label +
                "' stop_reason='bridge_exception' exception_summary='" + e.what() + "'";
            {
                std::lock_guard<std::mutex> lk(bridge_error_mutex_);
                if (!has_bridge_error_)
                {
                    has_bridge_error_ = true;
                    bridge_error_message_ = message;
                }
            }
            error(message);
            link.target->close();
            break;
        }
        catch (...)
        {
            const std::string message = "pipeline='" + pipeline_name() + "' node='" +
                                        name() + "' edge='" + link.source_label + "->" +
                                        link.target_label +
                                        "' stop_reason='bridge_exception' "
                                        "exception_summary='unknown exception'";
            {
                std::lock_guard<std::mutex> lk(bridge_error_mutex_);
                if (!has_bridge_error_)
                {
                    has_bridge_error_ = true;
                    bridge_error_message_ = message;
                }
            }
            error(message);
            link.target->close();
            break;
        }
    }
}
} // namespace takt
