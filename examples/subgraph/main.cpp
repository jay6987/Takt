// Demonstrates a nested increment pipeline with bound subgraph ports.
#include <iostream>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "takt/takt.h"

namespace
{
class DoubleNode final : public takt::Node
{
  public:
    // Binds input and output pipes to the example's doubling stage.
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

    // Doubles and publishes a frame while preserving shot-end markers.
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

// Executes direct or bridged nesting and verifies the resulting frame values.
int main(int argc,
         char** argv)
{
    const bool bridge = argc == 2 && std::string(argv[1]) == "bridge";
    if (argc > 2 || (argc == 2 && !bridge))
    {
        std::cerr << "usage: takt_subgraph_example [bridge]\n";
        return 1;
    }
    auto outer_in = std::make_shared<takt::Pipe<int>>("outer-in");
    outer_in->set_producer(
        {"source", takt::concurrency(1), takt::frames_per_write(1)}, 0);
    outer_in->set_consumer({bridge ? "input-bridge" : "double",
                            takt::concurrency(1), takt::frames_per_read(1),
                            takt::overlap_frames(0)});
    auto outer_out = std::make_shared<takt::Pipe<int>>("outer-out");
    outer_out->set_producer({bridge ? "output-bridge" : "double",
                             takt::concurrency(1), takt::frames_per_write(1)},
                            0);
    outer_out->set_consumer({"sink", takt::concurrency(1),
                             takt::frames_per_read(1),
                             takt::overlap_frames(0)});
    auto inner_in = outer_in;
    auto inner_out = outer_out;
    if (bridge)
    {
        inner_in = std::make_shared<takt::Pipe<int>>("inner-in");
        inner_in->set_producer(
            {"input-bridge", takt::concurrency(1), takt::frames_per_write(1)},
            0);
        inner_in->set_consumer({"double", takt::concurrency(1),
                                takt::frames_per_read(1),
                                takt::overlap_frames(0)});
        inner_out = std::make_shared<takt::Pipe<int>>("inner-out");
        inner_out->set_producer(
            {"double", takt::concurrency(1), takt::frames_per_write(1)}, 0);
        inner_out->set_consumer({"output-bridge", takt::concurrency(1),
                                 takt::frames_per_read(1),
                                 takt::overlap_frames(0)});
        inner_in->set_slot_capacity(16);
        inner_out->set_slot_capacity(16);
    }
    outer_in->set_slot_capacity(16);
    outer_out->set_slot_capacity(16);

    auto inner = takt::PipelineBuilder::declare("inner");
    inner.add_node("worker");
    inner.add_input_port(
        "worker", "in",
        {{"pipe_endpoint", takt::make_pipe_endpoint<int>(inner_in)}});
    inner.add_output_port(
        "worker", "out",
        {{"pipe_endpoint", takt::make_pipe_endpoint<int>(inner_out)}});
    inner.bind_node("worker",
                    std::make_shared<DoubleNode>(inner_in, inner_out));

    auto outer = takt::PipelineBuilder::declare("outer");
    outer.add_node("sg");
    outer.add_input_port(
        "sg", "raw",
        {{"pipe_endpoint", takt::make_pipe_endpoint<int>(outer_in)}});
    outer.add_output_port(
        "sg", "result",
        {{"pipe_endpoint", takt::make_pipe_endpoint<int>(outer_out)}});
    outer.bind_subgraph("sg", inner, {{"raw", {"worker", "in"}}},
                        {{"result", {"worker", "out"}}});

    auto runtime = outer.build_runtime();
    runtime->initialize();
    runtime->start();

    for (int i = 1; i <= 4; ++i)
    {
        auto w = outer_in->acquire_shot_end_write(1);
        w.value() = i;
        w.publish();
    }
    outer_in->close();

    runtime->join();

    std::vector<int> result;
    while (true)
    {
        try
        {
            auto r = outer_out->acquire_read();
            result.push_back(r.value());
        }
        catch (const takt::PipeClosedAndEmptySignal&)
        {
            break;
        }
    }

    std::cout << "subgraph outputs:";
    for (int v : result)
    {
        std::cout << " " << v;
    }
    std::cout << std::endl;

    return result == std::vector<int>{2, 4, 6, 8} ? 0 : 1;
}
