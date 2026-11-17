// Declares the processing node bound inside the nested subgraph example.
#pragma once

#include "takt/takt.h"

#include <memory>

namespace takt_example
{
/// Doubles nested-subgraph frames without losing shot-end boundaries.
class DoubleNode final : public takt::Node
{
  public:
    DoubleNode(std::shared_ptr<takt::Pipe<int>> input,
               std::shared_ptr<takt::Pipe<int>> output);
    ~DoubleNode() override;

    void request_stop() noexcept override;

  protected:
    /// Owns the read and write tokens for one admitted frame.
    struct DoubleTask final : public takt::Node::Task
    {
        takt::Pipe<int>::ReadToken input;
        takt::Pipe<int>::WriteToken output;
    };

    std::unique_ptr<takt::Node::Task> build_task(uint64_t sequence_id) override;
    void process_task(size_t worker,
                      takt::Node::Task& task) override;
    void on_ingress_manager_done() override;
    void on_exception(const takt::NodeExceptionContext& context) override;

  private:
    std::shared_ptr<takt::Pipe<int>> input_;
    std::shared_ptr<takt::Pipe<int>> output_;
};
} // namespace takt_example
