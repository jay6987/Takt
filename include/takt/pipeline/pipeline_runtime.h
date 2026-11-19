// Coordinates initialization, execution and shutdown of a bound pipeline.
#pragma once

#include <exception>
#include <memory>
#include <vector>

#include "takt/pipeline/runtime_statistics.h"

namespace takt
{
class NodeBase;
class PipeBase;

/// Owns the lifecycle of the nodes and pipes in a bound pipeline.
class PipelineRuntime
{
  public:
    // Owns bound nodes and lifecycle pipes without starting execution.
    explicit PipelineRuntime(std::vector<std::shared_ptr<NodeBase>> nodes,
                             std::vector<std::shared_ptr<PipeBase>> pipes = {});

    // Runtime lifecycle state cannot be copied.
    PipelineRuntime(const PipelineRuntime&) = delete;
    // Prevents replacing a live runtime with another runtime's state.
    PipelineRuntime& operator=(const PipelineRuntime&) = delete;

    // Registers an additional lifecycle pipe before initialization.
    void add_pipe(std::shared_ptr<PipeBase> pipe);
    // Validates and initializes the complete nested runtime tree.
    void initialize();
    // Reports whether this runtime completed initialization.
    bool initialized() const noexcept;
    // Starts initialized nodes and handles partial startup failures.
    void start();
    // Joins nodes, propagating remembered runtime failures.
    void join();
    // Requests node cancellation and closes all lifecycle pipes.
    void request_stop();
    // Enables statistics before start, with no concurrent pipe operations.
    void enable_statistics();
    // Reads a snapshot after join (including failed runs); empty when disabled.
    RuntimeStatistics statistics() const;

  private:
    enum class State
    {
        Created,
        Initialized,
        Started,
        Failed,
        Stopped
    };
    // Collects nested runtimes and pipes while rejecting invalid tree reuse.
    void collect_tree(std::vector<PipelineRuntime*>& runtimes,
                      std::vector<std::shared_ptr<PipeBase>>& pipes);
    // Rejects node ownership shared between runtimes.
    static void validate_nodes(const std::vector<PipelineRuntime*>& runtimes);
    // Validates all unique pipes before initializing any of them.
    static void
    initialize_pipes(const std::vector<std::shared_ptr<PipeBase>>& pipes);
    // Validates the complete initialized tree before launching nodes.
    void validate_start();
    // Stops and joins after a partial startup failure.
    [[noreturn]] void handle_start_failure(std::exception_ptr failure);
    void append_statistics(const std::string& prefix,
                           RuntimeStatistics& result,
                           std::vector<const PipeBase*>& seen_pipes) const;

    std::vector<std::shared_ptr<NodeBase>> nodes_;
    std::vector<std::shared_ptr<PipeBase>> pipes_;
    State state_ = State::Created;
    std::exception_ptr initialization_failure_;
    enum class StatisticsMode { Disabled, Root, Child };
    StatisticsMode statistics_mode_ = StatisticsMode::Disabled;
    bool start_attempted_ = false;
    bool joined_ = false;
};
} // namespace takt