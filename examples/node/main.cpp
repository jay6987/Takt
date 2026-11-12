// Demonstrates source, processing and sink nodes over configured frame pipes.
#include <iostream>
#include <memory>
#include <utility>
#include <vector>

#include "takt/takt.h"

namespace
{
class DoubleNode final : public takt::Node
{
  public:
    // Binds input and output pipes to a single-worker doubling stage.
    DoubleNode(std::shared_ptr<takt::Pipe<int>> in,
               std::shared_ptr<takt::Pipe<int>> out)
        : takt::Node("double-node"),
          in_(std::move(in)),
          out_(std::move(out))
    {
    }

    // Finish callbacks before destroying the bound pipe references.
    ~DoubleNode() override
    {
        request_stop();
        join();
    }

    // Wake ingress if it is waiting for an input or output token.
    void request_stop() noexcept override
    {
        Node::request_stop();
        in_->close();
        out_->close();
    }

  protected:
    struct DoubleTask final : public takt::Node::Task
    {
        takt::Pipe<int>::ReadToken input;
        takt::Pipe<int>::WriteToken output;
    };

    // Reserves both tokens on ingress only after a worker is available.
    std::unique_ptr<takt::Node::Task> build_task(uint64_t) override
    {
        auto task = std::make_unique<DoubleTask>();
        try
        {
            task->input = in_->acquire_read();
        }
        catch (const takt::PipeClosedAndEmptySignal&)
        {
            return nullptr;
        }
        task->output = task->input.is_shot_end()
                           ? out_->acquire_shot_end_write(1)
                           : out_->acquire_write();
        return task;
    }

    // Doubles and publishes the task payload while preserving shot markers.
    void process_task(size_t,
                      takt::Node::Task& task) override
    {
        auto& t = static_cast<DoubleTask&>(task);
        t.output.value() = t.input.value() * 2;
        t.output.publish();
    }

    // Close output only after every admitted task has drained.
    void on_ingress_manager_done() override
    {
        out_->close();
    }

    // Fault both boundaries before failing task tokens are released.
    void on_exception(const takt::NodeExceptionContext& context) override
    {
        in_->close_with_error(context.message);
        out_->close_with_error(context.message);
        Node::on_exception(context);
    }

  private:
    std::shared_ptr<takt::Pipe<int>> in_;
    std::shared_ptr<takt::Pipe<int>> out_;
};
} // namespace

// Executes the doubling stage and verifies its public pipe outputs.
int main()
{
    auto in = std::make_shared<takt::Pipe<int>>("node-in");
    in->set_producer(
        {"source", takt::concurrency(1), takt::frames_per_write(1)}, 0);
    in->set_consumer({"double", takt::concurrency(1), takt::frames_per_read(1),
                      takt::overlap_frames(0)});
    in->set_slot_capacity(16);
    auto out = std::make_shared<takt::Pipe<int>>("node-out");
    out->set_producer(
        {"double", takt::concurrency(1), takt::frames_per_write(1)}, 0);
    out->set_consumer({"sink", takt::concurrency(1), takt::frames_per_read(1),
                       takt::overlap_frames(0)});
    out->set_slot_capacity(16);
    in->initialize();
    out->initialize();

    DoubleNode node(in, out);
    node.set_stage_name("double-stage");
    node.add_input_port_name("raw");
    node.add_output_port_name("result");

    node.start();
    for (int i = 1; i <= 4; ++i)
    {
        auto w = in->acquire_shot_end_write(1);
        w.value() = i;
        w.publish();
    }
    in->close();
    node.join();

    std::vector<int> result;
    while (true)
    {
        try
        {
            auto r = out->acquire_read();
            result.push_back(r.value());
        }
        catch (const takt::PipeClosedAndEmptySignal&)
        {
            break;
        }
    }

    std::cout << "node outputs:";
    for (int v : result)
    {
        std::cout << " " << v;
    }
    std::cout << std::endl;

    return result == std::vector<int>{2, 4, 6, 8} ? 0 : 1;
}
