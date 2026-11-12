// Reserves idle workers and accounts for tasks through payload cleanup.
#include "takt/pipeline/node.h"

namespace takt
{
bool Node::wait_for_task_admission(size_t& worker_index)
{
    std::unique_lock<std::mutex> lk(admission_mutex_);
    admission_cv_.wait(lk, [this]
                       { return admission_closed_ || !idle_workers_.empty(); });
    if (admission_closed_)
        return false;
    worker_index = idle_workers_.front();
    idle_workers_.pop_front();
    return true;
}

bool Node::pop_task(size_t worker_index,
                    std::unique_ptr<Task>& task)
{
    std::unique_lock<std::mutex> lk(admission_mutex_);
    if (!admission_closed_)
    {
        idle_workers_.push_back(worker_index);
        admission_cv_.notify_all();
    }
    admission_cv_.wait(
        lk, [this, worker_index]
        { return admission_closed_ || task_slots_[worker_index] != nullptr; });
    if (!task_slots_[worker_index])
        return false;
    task = std::move(task_slots_[worker_index]);
    return true;
}

void Node::close_task_admission() noexcept
{
    {
        std::lock_guard<std::mutex> lk(admission_mutex_);
        admission_closed_ = true;
    }
    admission_cv_.notify_all();
}

void Node::discard_task_slots()
{
    std::vector<std::unique_ptr<Task>> discarded(worker_count_);
    size_t count = 0;
    {
        std::lock_guard<std::mutex> lk(admission_mutex_);
        admission_closed_ = true;
        for (size_t i = 0; i < worker_count_; ++i)
        {
            if (task_slots_[i])
            {
                discarded[i] = std::move(task_slots_[i]);
                ++count;
            }
        }
    }
    discarded.clear();
    {
        std::lock_guard<std::mutex> lk(admission_mutex_);
        outstanding_tasks_ -= count;
    }
    admission_cv_.notify_all();
}

void Node::wait_for_tasks()
{
    std::unique_lock<std::mutex> lock(admission_mutex_);
    admission_cv_.wait(lock, [this] { return outstanding_tasks_ == 0; });
}

void Node::finish_task()
{
    {
        std::lock_guard<std::mutex> lock(admission_mutex_);
        --outstanding_tasks_;
    }
    admission_cv_.notify_all();
}
} // namespace takt
