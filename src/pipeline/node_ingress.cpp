// Builds admitted tasks and drains barriers before acquiring another payload.
#include "takt/pipeline/node.h"

namespace takt
{
void Node::run_ingress_manager_wrapped()
{
    try
    {
        run_ingress_manager();
    }
    catch (...)
    {
        handle_exception(false, 0);
    }
    close_task_admission();
    if (stop_requested())
        discard_task_slots();
    wait_for_tasks();
    try
    {
        on_ingress_manager_done();
    }
    catch (...)
    {
        handle_exception(false, 0);
    }
}

void Node::run_ingress_manager()
{
    uint64_t sequence_id = 0;
    while (!stop_requested())
    {
        size_t worker_index = 0;
        if (!wait_for_task_admission(worker_index))
            break;
        auto task = build_task(sequence_id);
        if (!task)
            break;
        task->sequence_id = sequence_id;
        const bool barrier = task->barrier;
        if (!hand_off_task(worker_index, task))
            break;
        if (barrier)
        {
            wait_for_tasks();
            if (stop_requested() || failure())
                break;
            on_barrier_complete(sequence_id);
        }
        ++sequence_id;
    }
}

bool Node::hand_off_task(size_t worker_index,
                         std::unique_ptr<Task>& task)
{
    {
        std::lock_guard<std::mutex> lk(admission_mutex_);
        if (admission_closed_)
            return false;
        task_slots_[worker_index] = std::move(task);
        ++outstanding_tasks_;
    }
    admission_cv_.notify_all();
    return true;
}
} // namespace takt
