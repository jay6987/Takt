#pragma once

#include <memory>
#include <vector>

namespace takt
{
class NodeBase;

class PipelineRuntime
{
  public:
    explicit PipelineRuntime(std::vector<std::shared_ptr<NodeBase>> nodes);

    PipelineRuntime(const PipelineRuntime&) = delete;
    PipelineRuntime& operator=(const PipelineRuntime&) = delete;

    void start();
    void join();
    void request_stop();

  private:
    std::vector<std::shared_ptr<NodeBase>> nodes_;
    bool started_ = false;
};
} // namespace takt