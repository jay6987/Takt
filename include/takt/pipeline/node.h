#pragma once

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <future>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

namespace takt
{
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
    explicit NodeBase(std::string node_name);
    virtual ~NodeBase();

    NodeBase(const NodeBase&) = delete;
    NodeBase& operator=(const NodeBase&) = delete;

    const std::string& name() const noexcept;

    virtual void start() = 0;
    virtual void join() = 0;

    void set_stage_name(std::string stage_name);
    void set_pipeline_name(std::string pipeline_name);
    void add_input_port_name(std::string input_port_name);
    void add_output_port_name(std::string output_port_name);
    void clear_input_port_names() noexcept;
    void clear_output_port_names() noexcept;

    const std::string& stage_name() const noexcept;
    const std::string& pipeline_name() const noexcept;
    const std::vector<std::string>& input_port_names() const noexcept;
    const std::vector<std::string>& output_port_names() const noexcept;

    virtual void request_stop() noexcept;
    bool stop_requested() const noexcept;

  protected:
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
        virtual ~Task() = default;
    };

    explicit Node(std::string node_name, size_t worker_count = 1);
    ~Node() override;

    void start() override final;
    void join() override final;
    void request_stop() noexcept override;

    size_t worker_count() const noexcept;

  protected:
    virtual std::unique_ptr<Task> build_task(uint64_t sequence_id) = 0;
    virtual void process_task(size_t worker_index, Task& task) = 0;
    virtual void on_task_done(size_t worker_index, const Task& task);
    virtual void on_ingress_manager_done();
    virtual void on_worker_done(size_t worker_index);
    virtual void on_exception(const NodeExceptionContext& context);

  private:
    void run_ingress_manager_wrapped();
    void run_worker_wrapped(size_t worker_index);
    void close_task_queue() noexcept;
    bool pop_task(std::unique_ptr<Task>& task);

    size_t worker_count_;
    std::future<void> ingress_manager_;
    std::vector<std::future<void>> workers_;
    std::deque<std::unique_ptr<Task>> task_queue_;
    std::mutex task_queue_mutex_;
    std::condition_variable task_queue_cv_;
    bool task_queue_closed_ = false;
};
} // namespace takt
