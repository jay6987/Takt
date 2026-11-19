// Starts and joins permanent workers and exposes optional completion hooks.
#include "takt/pipeline/node.h"

#include <stdexcept>
#include <utility>

namespace takt
{
Node::Node(std::string node_name,
           size_t worker_count)
    : NodeBase(std::move(node_name)),
      worker_count_(worker_count),
      task_slots_(worker_count)
{
    if (worker_count_ == 0)
    {
        throw std::invalid_argument("worker_count cannot be zero");
    }
}

Node::~Node()
{
    request_stop();
    join();
}

void Node::start()
{
    if (ingress_manager_.valid() || !workers_.empty())
    {
        return;
    }
    {
        std::lock_guard<std::mutex> lk(admission_mutex_);
        idle_workers_.clear();
        admission_closed_ = false;
    }
    ingress_manager_ = std::async(std::launch::async,
                                  &Node::run_ingress_manager_wrapped, this);
    if (statistics_)
        statistics_started_ = true;
    workers_.reserve(worker_count_);
    for (size_t i = 0; i < worker_count_; ++i)
    {
        workers_.emplace_back(
            std::async(std::launch::async, &Node::run_worker_wrapped, this, i));
    }
}

void Node::join()
{
    if (ingress_manager_.valid())
    {
        ingress_manager_.get();
    }
    for (auto& worker : workers_)
    {
        if (worker.valid())
        {
            worker.get();
        }
    }
    workers_.clear();
}

void Node::request_stop() noexcept
{
    NodeBase::request_stop();
    close_task_admission();
}

size_t Node::worker_count() const noexcept
{
    return worker_count_;
}

void Node::on_task_done(size_t worker_index,
                        const Task& task)
{
    (void)worker_index;
    (void)task;
}

void Node::on_ingress_manager_done() {}

void Node::on_barrier_complete(uint64_t) {}

void Node::on_worker_done(size_t worker_index)
{
    (void)worker_index;
}
} // namespace takt
