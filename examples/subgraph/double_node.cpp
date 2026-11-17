// Implements token-owned frame processing for the nested subgraph example.
#include "double_node.h"

#include <utility>

namespace takt_example
{
// Binds the inner processing stage to its input and output frame pipes.
DoubleNode::DoubleNode(std::shared_ptr<takt::Pipe<int>> input,
                       std::shared_ptr<takt::Pipe<int>> output)
    : takt::Node("double-node"),
      input_(std::move(input)),
      output_(std::move(output))
{
}

// Stops and joins processing before the bound pipes are released.
DoubleNode::~DoubleNode()
{
    request_stop();
    join();
}

// Cancels admission and wakes operations blocked on either boundary pipe.
void DoubleNode::request_stop() noexcept
{
    Node::request_stop();
    input_->close();
    output_->close();
}

// Acquires matching frame tokens while preserving input shot boundaries.
std::unique_ptr<takt::Node::Task> DoubleNode::build_task(uint64_t)
{
    auto task = std::make_unique<DoubleTask>();
    try
    {
        task->input = input_->acquire_read();
    }
    catch (const takt::PipeClosedAndEmptySignal&)
    {
        return nullptr;
    }
    task->output = task->input.is_shot_end()
                       ? output_->acquire_shot_end_write(1)
                       : output_->acquire_write();
    return task;
}

// Doubles the admitted frame and publishes its output token.
void DoubleNode::process_task(size_t,
                              takt::Node::Task& base)
{
    auto& task = static_cast<DoubleTask&>(base);
    task.output.value() = task.input.value() * 2;
    task.output.publish();
}

// Signals downstream completion after all admitted tasks have drained.
void DoubleNode::on_ingress_manager_done()
{
    output_->close();
}

// Faults both boundary pipes before reporting the original node failure.
void DoubleNode::on_exception(const takt::NodeExceptionContext& context)
{
    input_->close_with_error(context.message);
    output_->close_with_error(context.message);
    Node::on_exception(context);
}
} // namespace takt_example
