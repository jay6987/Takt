// Defines lazy node ingress, permanent workers and completion barriers.
#pragma once

#include "takt/pipeline/node_base.h"
#include "takt/pipeline/runtime_statistics.h"

#include <condition_variable>
#include <cstdint>
#include <deque>
#include <exception>
#include <future>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

namespace takt
{
/// Runs lazy ingress and permanent workers with destruction-aware barriers.
/// Derived destructors must stop and join before destroying borrowed state.
class Node : public NodeBase
{
  public:
    enum class TaskKind
    {
        Normal,
        Stop,
    };

    /// Owns an admitted payload until processing and completion hooks finish.
    struct Task
    {
        uint64_t sequence_id = 0;
        TaskKind kind = TaskKind::Normal;
        // Drain this task and all predecessors before ingress builds another.
        bool barrier = false;
        // Allows derived task payloads to be destroyed through the base.
        virtual ~Task() = default;
    };

    // Configures permanent workers with no queued-task backlog.
    explicit Node(std::string node_name,
                  size_t worker_count = 1);
    // Requests stop and joins any remaining asynchronous execution.
    ~Node() override;

    // Launches the ingress manager and permanent worker set.
    void start() override final;
    // Waits for ingress and workers; failure() retains execution errors.
    void join() override final;
    // Requests stop and wakes blocked admission and worker operations.
    void request_stop() noexcept override;

    // Returns the configured number of permanent processing workers.
    size_t worker_count() const noexcept;

  protected:
    // Waits for handed-off/running tasks and payload destruction to finish.
    void wait_for_tasks();
    // Acquires input and builds a task for the next ingress sequence.
    virtual std::unique_ptr<Task> build_task(uint64_t sequence_id) = 0;
    // Processes one task on the indicated permanent worker.
    virtual void process_task(size_t worker_index,
                              Task& task) = 0;
    // Runs after successful processing, before the task is destroyed.
    virtual void on_task_done(size_t worker_index,
                              const Task& task);
    // Runs on ingress after a barrier and all preceding payloads are destroyed.
    virtual void on_barrier_complete(uint64_t sequence_id);
    // Runs after admission ends and all admitted payloads are destroyed.
    virtual void on_ingress_manager_done();
    // Runs when the indicated processing worker terminates.
    virtual void on_worker_done(size_t worker_index);
    // Reports structured execution failure context through the logger.
    virtual void on_exception(const NodeExceptionContext& context);

  private:
    friend class PipelineRuntime;
    // Drives ingress admission while retaining and reporting failures.
    void run_ingress_manager_wrapped();
    // Builds tasks only after admission and drains each barrier before reuse.
    void run_ingress_manager(IngressStatistics* statistics);
    // Publishes one task unless cancellation closed its reserved worker slot.
    bool hand_off_task(size_t worker_index,
                       std::unique_ptr<Task>& task);
    // Drives one worker while retaining and reporting failures.
    void run_worker_wrapped(size_t worker_index);
    // Processes and destroys one payload before releasing its drain count.
    bool process_admitted_task(size_t worker_index,
                               std::unique_ptr<Task>& task,
                               WorkerStatistics* statistics);
    // Closes task admission and wakes waiting workers.
    void close_task_admission() noexcept;
    // Releases unclaimed task slots outside the coordination lock.
    void discard_task_slots();
    // Releases an admitted task's drain count after its payload is destroyed.
    void finish_task();
    // Announces worker availability and waits for its reserved task or closure.
    bool pop_task(size_t worker_index,
                  std::unique_ptr<Task>& task);
    // Reserves an idle worker before ingress acquires tokens or builds a task.
    bool wait_for_task_admission(size_t& worker_index);
    // Retains and reports the active exception before task token cleanup.
    void handle_exception(bool has_worker_index,
                          size_t worker_index) noexcept;
    // Converts a retained cause into standard or unknown failure diagnostics.
    NodeExceptionContext describe_exception(std::exception_ptr cause,
                                            bool has_worker_index,
                                            size_t worker_index) const;

    size_t worker_count_;
    std::future<void> ingress_manager_;
    std::vector<std::future<void>> workers_;
    std::vector<std::unique_ptr<Task>> task_slots_;
    std::deque<size_t> idle_workers_;
    std::mutex admission_mutex_;
    std::condition_variable admission_cv_;
    bool admission_closed_ = false;
    size_t outstanding_tasks_ = 0;
    std::unique_ptr<NodeStatistics> statistics_;
    bool statistics_started_ = false;
};
} // namespace takt
