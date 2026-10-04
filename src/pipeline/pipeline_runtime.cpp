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
    for (auto& node : nodes_)
    {
        node->join();
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