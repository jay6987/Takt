// Accounts for admitted tasks until payload cleanup, including abandoned work.
#include "takt/pipeline/node.h"

namespace takt
{
bool Node::wait_for_task_admission()
{
    std::unique_lock<std::mutex> lk(task_queue_mutex_);
    task_queue_cv_.wait(lk,
                        [this]
                        {
                            return task_queue_closed_ ||
                                   task_queue_.size() < task_queue_capacity_;
                        });
    return !task_queue_closed_;
}

bool Node::pop_task(std::unique_ptr<Task>& task)
{
    std::unique_lock<std::mutex> lk(task_queue_mutex_);
    task_queue_cv_.wait(lk, [this]
                        { return task_queue_closed_ || !task_queue_.empty(); });
    if (task_queue_.empty())
        return false;
    task = std::move(task_queue_.front());
    task_queue_.pop_front();
    lk.unlock();
    task_queue_cv_.notify_all();
    return true;
}

void Node::close_task_queue() noexcept
{
    {
        std::lock_guard<std::mutex> lk(task_queue_mutex_);
        task_queue_closed_ = true;
    }
    task_queue_cv_.notify_all();
}

void Node::discard_task_queue()
{
    std::deque<std::unique_ptr<Task>> discarded;
    {
        std::lock_guard<std::mutex> lk(task_queue_mutex_);
        task_queue_closed_ = true;
        discarded.swap(task_queue_);
    }
    const auto count = discarded.size();
    discarded.clear();
    {
        std::lock_guard<std::mutex> lk(task_queue_mutex_);
        outstanding_tasks_ -= count;
    }
    task_queue_cv_.notify_all();
}

void Node::wait_for_tasks()
{
    std::unique_lock<std::mutex> lock(task_queue_mutex_);
    task_queue_cv_.wait(lock, [this] { return outstanding_tasks_ == 0; });
}

void Node::finish_task()
{
    {
        std::lock_guard<std::mutex> lock(task_queue_mutex_);
        --outstanding_tasks_;
    }
    task_queue_cv_.notify_all();
}
} // namespace takt
