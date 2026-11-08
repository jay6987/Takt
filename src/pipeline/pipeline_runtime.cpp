#include "takt/pipeline/pipeline_runtime.h"

#include <algorithm>
#include <stdexcept>
#include <unordered_set>
#include <utility>

#include "takt/logging/logger.h"
#include "takt/pipeline/node.h"
#include "takt/pipeline/pipe_base.h"

namespace takt
{
PipelineRuntime::PipelineRuntime(std::vector<std::shared_ptr<NodeBase>> nodes,
                                 std::vector<std::shared_ptr<PipeBase>> pipes)
    : nodes_(std::move(nodes)), pipes_(std::move(pipes))
{
    for (const auto& node : nodes_)
        if (!node)
            throw std::invalid_argument("runtime node cannot be null");
    for (const auto& pipe : pipes_)
        if (!pipe)
            throw std::invalid_argument("runtime pipe cannot be null");
}

void PipelineRuntime::add_pipe(std::shared_ptr<PipeBase> pipe)
{
    if (state_ != State::Created)
        throw std::logic_error("runtime pipe registration is frozen");
    if (!pipe)
        throw std::invalid_argument("runtime pipe cannot be null");
    pipes_.push_back(std::move(pipe));
}

bool PipelineRuntime::initialized() const noexcept
{
    return state_ == State::Initialized || state_ == State::Started;
}

void PipelineRuntime::collect_tree(std::vector<PipelineRuntime*>& runtimes,
                                   std::vector<std::shared_ptr<PipeBase>>& pipes)
{
    if (std::find(runtimes.begin(), runtimes.end(), this) != runtimes.end())
        throw std::logic_error("runtime tree contains a cycle or shared subgraph");
    if (state_ == State::Started || state_ == State::Failed || state_ == State::Stopped)
        throw std::logic_error("runtime is not available for initialization");
    runtimes.push_back(this);
    pipes.insert(pipes.end(), pipes_.begin(), pipes_.end());
    std::unordered_set<NodeBase*> nodes;
    for (const auto& node : nodes_)
    {
        if (!nodes.insert(node.get()).second)
            throw std::logic_error("runtime contains duplicate node ownership");
        for (const auto& child : node->child_runtimes())
        {
            if (!child)
                throw std::logic_error("subgraph runtime cannot be null");
            child->collect_tree(runtimes, pipes);
        }
    }
}

void PipelineRuntime::initialize()
{
    if (state_ != State::Created)
        throw std::logic_error("runtime initialization is one-shot");
    std::vector<PipelineRuntime*> runtimes;
    std::vector<std::shared_ptr<PipeBase>> pipes;
    try
    {
        collect_tree(runtimes, pipes);
        std::unordered_set<NodeBase*> nodes;
        for (const auto* runtime : runtimes)
            for (const auto& node : runtime->nodes_)
                if (!nodes.insert(node.get()).second)
                    throw std::logic_error("node is shared by multiple runtimes");
        std::unordered_set<PipeBase*> seen;
        std::vector<std::shared_ptr<PipeBase>> unique;
        for (const auto& pipe : pipes)
        {
            if (!seen.insert(pipe.get()).second)
                continue;
            if (pipe->closed() || pipe->faulted())
                throw std::logic_error("pipe='" + pipe->name() +
                                       "': terminal pipe cannot initialize a runtime");
            if (!pipe->initialized())
                pipe->validate_configuration();
            unique.push_back(pipe);
        }
        for (const auto& pipe : unique)
            if (!pipe->initialized())
                pipe->initialize();
        for (auto* runtime : runtimes)
            runtime->state_ = State::Initialized;
    }
    catch (...)
    {
        initialization_failure_ = std::current_exception();
        state_ = State::Failed;
        for (auto* runtime : runtimes)
        {
            runtime->initialization_failure_ = initialization_failure_;
            runtime->state_ = State::Failed;
        }
        throw;
    }
}

void PipelineRuntime::start()
{
    if (state_ == State::Failed && initialization_failure_)
        std::rethrow_exception(initialization_failure_);
    if (state_ == State::Started)
    {
        return;
    }
    if (state_ != State::Initialized)
        throw std::logic_error("runtime must be initialized before start");
    std::vector<PipelineRuntime*> runtimes;
    std::vector<std::shared_ptr<PipeBase>> pipes;
    collect_tree(runtimes, pipes);
    for (const auto* runtime : runtimes)
        if (runtime->state_ != State::Initialized)
            throw std::logic_error(
                "entire runtime tree must be initialized before start");
    for (const auto& pipe : pipes)
        if (!pipe->initialized())
            throw std::logic_error("pipe='" + pipe->name() +
                                   "': pipe is not initialized");
    try
    {
        for (auto& node : nodes_)
            node->start();
        state_ = State::Started;
    }
    catch (...)
    {
        const auto error = std::current_exception();
        state_ = State::Failed;
        initialization_failure_ = error;
        request_stop();
        try
        {
            join();
        }
        catch (const std::exception& cleanup)
        {
            takt::error(std::string("runtime startup cleanup failed: ") +
                        cleanup.what());
        }
        catch (...)
        {
            takt::error("runtime startup cleanup failed: unknown exception");
        }
        std::rethrow_exception(error);
    }
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
    if (state_ != State::Failed)
        state_ = State::Stopped;
    std::exception_ptr close_failure;
    for (const auto& pipe : pipes_)
    {
        try
        {
            pipe->close();
        }
        catch (...)
        {
            if (!close_failure)
                close_failure = std::current_exception();
        }
    }
    for (auto& node : nodes_)
    {
        node->request_stop();
    }
    if (close_failure)
        std::rethrow_exception(close_failure);
}
} // namespace takt