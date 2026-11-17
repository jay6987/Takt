// Transfers and stops subgraph boundary bridges.
#include "takt/pipeline/subgraph_node.h"

#include "takt/logging/logger.h"
#include "takt/pipeline/pipeline_runtime.h"

namespace takt
{
void SubgraphNode::request_stop() noexcept
{
    NodeBase::request_stop();
    bridge_stop_requested_.store(true);
    for (const auto& link : mapping_links_)
    {
        try
        {
            link.source->close();
        }
        catch (const std::exception& e)
        {
            error("pipeline='" + pipeline_name() + "' node='" + name() +
                  "' failed to close bridge source: " + e.what());
        }
        try
        {
            link.target->close();
        }
        catch (const std::exception& e)
        {
            error("pipeline='" + pipeline_name() + "' node='" + name() +
                  "' failed to close bridge target: " + e.what());
        }
    }
    try
    {
        runtime_->request_stop();
    }
    catch (const std::exception& e)
    {
        error("pipeline='" + pipeline_name() + "' node='" + name() +
              "' failed to stop inner runtime: " + e.what());
    }
}

void SubgraphNode::fail_bridge(MappingLink& link,
                               const std::string& summary)
{
    const std::string message =
        "pipeline='" + pipeline_name() + "' node='" + name() + "' edge='" +
        link.source_label + "->" + link.target_label +
        "' stop_reason='bridge_exception' exception_summary='" + summary + "'";
    {
        std::lock_guard<std::mutex> lk(bridge_error_mutex_);
        if (!has_bridge_error_)
        {
            has_bridge_error_ = true;
            bridge_error_message_ = message;
        }
    }
    error(message);
    link.source->close_with_error(message);
    link.target->close_with_error(message);
}

void SubgraphNode::run_bridge(size_t index)
{
    auto& link = bridge_links_[index];
    while (!bridge_stop_requested_.load())
    {
        try
        {
            link.source->transfer_to(*link.target);
        }
        catch (const PipeClosedAndEmptySignal&)
        {
            link.target->close();
            break;
        }
        catch (const std::exception& e)
        {
            fail_bridge(link, e.what());
            break;
        }
        catch (...)
        {
            fail_bridge(link, "unknown exception");
            break;
        }
    }
}
} // namespace takt
