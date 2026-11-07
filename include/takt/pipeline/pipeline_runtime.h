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
    explicit PipelineRuntime(std::vector<std::shared_ptr<NodeBase>> nodes,
                             std::vector<std::shared_ptr<PipeBase>> pipes = {});

    PipelineRuntime(const PipelineRuntime&) = delete;
    PipelineRuntime& operator=(const PipelineRuntime&) = delete;

    void add_pipe(std::shared_ptr<PipeBase> pipe);
    void initialize();
    bool initialized() const noexcept;
    void start();
    void join();
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
    void collect_tree(std::vector<PipelineRuntime*>& runtimes,
                      std::vector<std::shared_ptr<PipeBase>>& pipes);

    std::vector<std::shared_ptr<NodeBase>> nodes_;
    std::vector<std::shared_ptr<PipeBase>> pipes_;
    State state_ = State::Created;
    std::exception_ptr initialization_failure_;
};
} // namespace takt