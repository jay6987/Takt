// Defines runtime node identity, diagnostic context, and cooperative failure.
#pragma once

#include "takt/pipeline/node_exception_context.h"

#include <atomic>
#include <exception>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

namespace takt
{
class PipelineRuntime;

/// Exposes node lifecycle and retains cancellation and the first failure.
/// Runtime bindings configure identity and port context before execution.
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
} // namespace takt
