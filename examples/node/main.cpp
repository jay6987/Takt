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

  protected:
    struct DoubleTask final : public takt::Node::Task
    {
        int value = 0;
        bool shot_end = false;
    };

    // Reads one frame into a task, closing output when input is drained.
    std::unique_ptr<takt::Node::Task> build_task(uint64_t) override
    {
        try
        {
            auto r = in_->acquire_read();
            auto task = std::make_unique<DoubleTask>();
            task->value = r.value();
            task->shot_end = r.is_shot_end();
            return task;
        }
        catch (const takt::PipeClosedAndEmptySignal&)
        {
            out_->close();
            return nullptr;
        }
    }

    // Doubles and publishes the task payload while preserving shot markers.
    void process_task(size_t,
                      takt::Node::Task& task) override
    {
        auto& t = static_cast<DoubleTask&>(task);
        auto w = t.shot_end ? out_->acquire_shot_end_write(1)
                            : out_->acquire_write();
        w.value() = t.value * 2;
        w.publish();
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
