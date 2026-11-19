// Processes reserved payloads and releases drain counts after destruction.
#include "takt/pipeline/node.h"

#include "statistics_timer.h"

namespace takt
{
void Node::run_worker_wrapped(size_t worker_index)
{
    WorkerStatistics totals;
    auto* statistics = statistics_ ? &totals : nullptr;
    try
    {
        std::unique_ptr<Task> task;
        while (true)
        {
            bool received;
            {
                const StatisticsTimer timer(
                    statistics ? &statistics->task_wait_time : nullptr);
                received = pop_task(worker_index, task);
            }
            if (!received || !process_admitted_task(worker_index, task, statistics))
                break;
        }
    }
    catch (...)
    {
        handle_exception(worker_count_ > 1, worker_index);
    }
    try
    {
        on_worker_done(worker_index);
    }
    catch (...)
    {
        handle_exception(worker_count_ > 1, worker_index);
    }
    if (statistics)
        statistics_->workers[worker_index] = totals;
}

bool Node::process_admitted_task(size_t worker_index,
                                 std::unique_ptr<Task>& task,
                                 WorkerStatistics* statistics)
{
    bool keep_running = false;
    if (stop_requested() || task->kind == TaskKind::Stop)
    {
        request_stop();
    }
    else
    {
        if (statistics)
            ++statistics->tasks_started;
        try
        {
            const StatisticsTimer timer(
                statistics ? &statistics->work_time : nullptr);
            process_task(worker_index, *task);
            on_task_done(worker_index, *task);
            keep_running = true;
            if (statistics)
                ++statistics->tasks_succeeded;
        }
        catch (...)
        {
            if (statistics)
                ++statistics->tasks_failed;
            handle_exception(worker_count_ > 1, worker_index);
        }
    }
    task.reset();
    finish_task();
    return keep_running;
}
} // namespace takt
