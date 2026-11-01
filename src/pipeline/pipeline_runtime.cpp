#include "takt/pipeline/pipeline_runtime.h"

#include <utility>

#include "takt/pipeline/node.h"

namespace takt
{
PipelineRuntime::PipelineRuntime(std::vector<std::shared_ptr<NodeBase>> nodes)
    : nodes_(std::move(nodes))
{
}

void PipelineRuntime::start()
{
    if (started_)
    {
        return;
    }
    for (auto& node : nodes_)
    {
        node->start();
    }
    started_ = true;
}

void PipelineRuntime::join()
{
    std::exception_ptr failure;
    for (auto& node : nodes_)
    {
        try
        {
            node->join();
            if (!failure)
            {
                failure = node->failure();
            }
        }
        catch (...)
        {
            if (!failure)
            {
                failure = std::current_exception();
            }
        }
    }
    if (failure)
    {
        std::rethrow_exception(failure);
    }
}

void PipelineRuntime::request_stop()
{
    for (auto& node : nodes_)
    {
        node->request_stop();
    }
}
} // namespace takt