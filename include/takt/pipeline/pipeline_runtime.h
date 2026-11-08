// Coordinates initialization, execution and shutdown of a bound pipeline.
#pragma once

#include <exception>
#include <memory>
#include <vector>

namespace takt
{
class NodeBase;
class PipeBase;

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

    std::vector<std::shared_ptr<NodeBase>> nodes_;
    std::vector<std::shared_ptr<PipeBase>> pipes_;
    State state_ = State::Created;
    std::exception_ptr initialization_failure_;
};
} // namespace takt