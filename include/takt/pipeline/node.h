// Defines lazy node ingress, permanent workers and completion barriers.
#pragma once

#include <atomic>
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
class PipelineRuntime;

struct NodeExceptionContext
{
    std::string pipeline_name;
    std::string node_name;
    std::string stage_name;
    std::vector<std::string> input_port_names;
    std::vector<std::string> output_port_names;
    std::string stop_reason;
    std::string message;
    bool unknown_exception = false;
    bool has_worker_index = false;
    size_t worker_index = 0;
};

class NodeBase
{
  public:
    // Establishes node identity without launching workers.
    explicit NodeBase(std::string node_name);
    // Releases the polymorphic node interface.
    virtual ~NodeBase();

    // Node lifecycle and failure state cannot be copied.
    NodeBase(const NodeBase&) = delete;
    // Prevents replacing a node referenced by runtime bindings.
    NodeBase& operator=(const NodeBase&) = delete;

    // Returns the node's diagnostic identity.
    const std::string& name() const noexcept;

    // Starts this initialized runtime node.
    virtual void start() = 0;
    // Waits for this node's execution to finish.
    virtual void join() = 0;
    // Exposes nested runtimes; ordinary nodes have no children.
    virtual std::vector<std::shared_ptr<PipelineRuntime>> child_runtimes() const
    {
        return {};
    }

    // Sets the logical stage reported in exception context.
    void set_stage_name(std::string stage_name);
    // Sets the owning pipeline name reported in exception context.
    void set_pipeline_name(std::string pipeline_name);
    // Adds an input-port label to exception context.
    void add_input_port_name(std::string input_port_name);
    // Adds an output-port label to exception context.
    void add_output_port_name(std::string output_port_name);
    // Removes previously bound input-port context.
    void clear_input_port_names() noexcept;
    // Removes previously bound output-port context.
    void clear_output_port_names() noexcept;

    // Returns the configured logical stage.
    const std::string& stage_name() const noexcept;
    // Returns the configured owning pipeline.
    const std::string& pipeline_name() const noexcept;
    // Returns bound input-port labels.
    const std::vector<std::string>& input_port_names() const noexcept;
    // Returns bound output-port labels.
    const std::vector<std::string>& output_port_names() const noexcept;

    // Atomically requests cooperative node cancellation.
    virtual void request_stop() noexcept;
    // Reports the cooperative cancellation flag.
    bool stop_requested() const noexcept;
    // Returns the first remembered exception under synchronization.
    std::exception_ptr failure() const;

  protected:
    // Retains the first execution failure for later runtime propagation.
    void remember_failure(std::exception_ptr failure);
    // Builds structured diagnostics for ingress or worker failures.
    NodeExceptionContext build_exception_context(std::string message,
                                                 bool unknown_exception,
                                                 bool has_worker_index,
                                                 size_t worker_index) const;

  private:
    std::string name_;
    std::string pipeline_name_;
    std::string stage_name_;
    std::vector<std::string> input_port_names_;
    std::vector<std::string> output_port_names_;
    std::atomic<bool> stop_requested_{false};
    mutable std::mutex failure_mutex_;
    std::exception_ptr failure_;
};

class Node : public NodeBase
{
  public:
    enum class TaskKind
    {
        Normal,
        Stop,
    };

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
    // Drives ingress admission while retaining and reporting failures.
    void run_ingress_manager_wrapped();
    // Drives one worker while retaining and reporting failures.
    void run_worker_wrapped(size_t worker_index);
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

    size_t worker_count_;
    std::future<void> ingress_manager_;
    std::vector<std::future<void>> workers_;
    std::vector<std::unique_ptr<Task>> task_slots_;
    std::deque<size_t> idle_workers_;
    std::mutex admission_mutex_;
    std::condition_variable admission_cv_;
    bool admission_closed_ = false;
    size_t outstanding_tasks_ = 0;
};
} // namespace takt
