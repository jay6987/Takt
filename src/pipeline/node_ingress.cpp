// Builds admitted tasks and drains barriers before acquiring another payload.
#include "takt/pipeline/node.h"

#include "statistics_timer.h"

namespace takt
{
void Node::run_ingress_manager_wrapped()
{
    IngressStatistics totals;
    auto* statistics = statistics_ ? &totals : nullptr;
    try
    {
        run_ingress_manager(statistics);
    }
    catch (...)
    {
        handle_exception(false, 0);
    }
    close_task_admission();
    if (stop_requested())
        discard_task_slots();
    {
        const StatisticsTimer timer(
            statistics ? &statistics->drain_wait_time : nullptr);
        wait_for_tasks();
    }
    try
    {
        on_ingress_manager_done();
    }
    catch (...)
    {
        handle_exception(false, 0);
    }
    if (statistics)
    {
        totals.task_build_time =
            totals.first_task_build_time + totals.in_round_task_build_time;
        statistics_->ingress = totals;
    }
}

void Node::run_ingress_manager(IngressStatistics* statistics)
{
    uint64_t sequence_id = 0;
    auto* build_time = statistics ? &statistics->first_task_build_time : nullptr;
    while (!stop_requested())
    {
        size_t worker_index = 0;
        bool admitted;
        {
            const StatisticsTimer timer(
                statistics ? &statistics->worker_wait_time : nullptr);
            admitted = wait_for_task_admission(worker_index);
        }
        if (!admitted)
            break;
        auto task = [&]
        {
            const StatisticsTimer timer(build_time);
            return build_task(sequence_id);
        }();
        if (!task)
            break;
        task->sequence_id = sequence_id;
        const bool barrier = task->barrier;
        if (!hand_off_task(worker_index, task))
            break;
        if (barrier)
        {
            {
                const StatisticsTimer timer(
                    statistics ? &statistics->drain_wait_time : nullptr);
                wait_for_tasks();
            }
            if (stop_requested() || failure())
                break;
            on_barrier_complete(sequence_id);
        }
        if (statistics)
            build_time = barrier ? &statistics->first_task_build_time
                                 : &statistics->in_round_task_build_time;
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
