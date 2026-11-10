// Retains frame ownership through bounded execution, completion and failures.
#include "takt/pipeline/frame_task_node.h"
#include "takt/logging/logger.h"

#include <algorithm>
#include <stdexcept>
#include <utility>

namespace takt
{
struct FrameTaskNode::GroupTask final : Task
{
    Frame* frame = nullptr;
    std::unique_ptr<Task> item;
};

FrameTaskNode::FrameTaskNode(std::string name,
                             size_t workers,
                             size_t queue_capacity)
    : Node(std::move(name),
           workers,
           queue_capacity)
{
}

FrameTaskNode::~FrameTaskNode()
{
    request_stop();
    join();
}

void FrameTaskNode::register_pipe(std::shared_ptr<PipeBase> pipe,
                                  bool output)
{
    if (!pipe)
    {
        throw std::invalid_argument("frame node pipe cannot be null");
    }
    if (std::find(pipes_.begin(), pipes_.end(), pipe) == pipes_.end())
    {
        pipes_.push_back(pipe);
    }
    if (output &&
        std::find(outputs_.begin(), outputs_.end(), pipe) == outputs_.end())
    {
        outputs_.push_back(std::move(pipe));
    }
}

void FrameTaskNode::request_stop() noexcept
{
    Node::request_stop();
    for (const auto& pipe : pipes_)
    {
        try
        {
            pipe->close();
        }
        catch (...)
        {
            remember_failure(std::current_exception());
        }
    }
}

size_t FrameTaskNode::completed_frames() const noexcept
{
    return completed_.load();
}

void FrameTaskNode::on_frame_start(Frame&) {}
void FrameTaskNode::on_frame_complete(Frame&) {}
void FrameTaskNode::on_frame_failure(std::exception_ptr) {}

void FrameTaskNode::abort_frame(const std::exception_ptr& error)
{
    remember_failure(error);
    try
    {
        on_frame_failure(error);
    }
    catch (...)
    {
        takt::error(
            "frame failure observer threw; preserving original failure");
    }
    std::string message = "unknown frame task exception";
    try
    {
        std::rethrow_exception(error);
    }
    catch (const std::exception& cause)
    {
        message = cause.what();
    }
    catch (...)
    {
        message =
            "nonstandard frame task exception (original exception retained)";
    }
    for (const auto& pipe : pipes_)
    {
        try
        {
            pipe->close_with_error(message);
        }
        catch (...)
        {
            takt::error(
                "frame pipe fault closure threw; preserving original failure");
        }
    }
    request_stop();
}

std::unique_ptr<Node::Task> FrameTaskNode::build_task(uint64_t)
{
    try
    {
        while (!stop_requested())
        {
            if (!current_)
            {
                current_ = create_frame();
                if (!current_)
                {
                    throw std::logic_error("frame context cannot be null");
                }
                current_->index = next_frame_;
                if (!acquire_frame(*current_))
                {
                    return nullptr;
                }
                if (stop_requested())
                {
                    return nullptr;
                }
                on_frame_start(*current_);
            }
            auto item = build_frame_task(*current_, next_task_);
            if (item)
            {
                item->sequence_id = next_task_++;
                auto task = std::make_unique<GroupTask>();
                task->frame = current_.get();
                task->item = std::move(item);
                return task;
            }
            wait_for_tasks();
            if (stop_requested() || failure())
            {
                return nullptr;
            }
            on_frame_complete(*current_);
            current_.reset();
            ++completed_;
            ++next_frame_;
            next_task_ = 0;
        }
        return nullptr;
    }
    catch (...)
    {
        abort_frame(std::current_exception());
        throw;
    }
}

void FrameTaskNode::process_task(size_t worker,
                                 Task& task)
{
    auto& group = static_cast<GroupTask&>(task);
    if (stop_requested())
    {
        return;
    }
    try
    {
        process_frame_task(worker, *group.frame, *group.item);
    }
    catch (...)
    {
        abort_frame(std::current_exception());
        throw;
    }
}

void FrameTaskNode::on_ingress_manager_done()
{
    wait_for_tasks();
    try
    {
        for (const auto& pipe : outputs_)
            pipe->close();
    }
    catch (...)
    {
        abort_frame(std::current_exception());
    }
    current_.reset();
}
} // namespace takt
