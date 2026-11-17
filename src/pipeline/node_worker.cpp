// Processes reserved payloads and releases drain counts after destruction.
#include "takt/pipeline/node.h"

namespace takt
{
void Node::run_worker_wrapped(size_t worker_index)
{
    try
    {
        std::unique_ptr<Task> task;
        while (pop_task(worker_index, task))
        {
            if (!process_admitted_task(worker_index, task))
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
}

bool Node::process_admitted_task(size_t worker_index,
                                 std::unique_ptr<Task>& task)
{
    bool keep_running = false;
    if (stop_requested() || task->kind == TaskKind::Stop)
    {
        request_stop();
    }
    else
    {
        try
        {
            process_task(worker_index, *task);
            on_task_done(worker_index, *task);
            keep_running = true;
        }
        catch (...)
        {
            handle_exception(worker_count_ > 1, worker_index);
        }
    }
    task.reset();
    finish_task();
    return keep_running;
}
} // namespace takt
