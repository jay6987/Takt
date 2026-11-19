// Initializes runtime trees and propagates lifecycle failures across nodes.
#include "takt/pipeline/pipeline_runtime.h"

#include <stdexcept>
#include <utility>

#include "takt/logging/logger.h"
#include "takt/pipeline/node.h"
#include "takt/pipeline/pipe_base.h"

namespace takt
{
PipelineRuntime::PipelineRuntime(std::vector<std::shared_ptr<NodeBase>> nodes,
                                 std::vector<std::shared_ptr<PipeBase>> pipes)
    : nodes_(std::move(nodes)),
      pipes_(std::move(pipes))
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
    validate_start();
    start_attempted_ = true;
    joined_ = false;
    try
    {
        for (auto& node : nodes_)
            node->start();
        state_ = State::Started;
    }
    catch (...)
    {
        handle_start_failure(std::current_exception());
    }
}

void PipelineRuntime::validate_start()
{
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
}

void PipelineRuntime::handle_start_failure(std::exception_ptr failure)
{
    state_ = State::Failed;
    initialization_failure_ = failure;
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
    std::rethrow_exception(failure);
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
    joined_ = start_attempted_;
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