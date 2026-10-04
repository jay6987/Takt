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
    DoubleNode(std::shared_ptr<takt::Pipe<int>> in,
               std::shared_ptr<takt::Pipe<int>> out)
        : takt::Node("double-node"), in_(std::move(in)), out_(std::move(out))
    {
    }

  protected:
    struct DoubleTask final : public takt::Node::Task
    {
        int value = 0;
        bool segment_end = false;
    };

    std::unique_ptr<takt::Node::Task> build_task(uint64_t) override
    {
        try
        {
            auto r = in_->acquire_read();
            auto task = std::make_unique<DoubleTask>();
            task->value = r.value();
            task->segment_end = r.is_segment_end();
            return task;
        }
        catch (const takt::PipeClosedAndEmptySignal&)
        {
            out_->close();
            return nullptr;
        }
    }

    void process_task(size_t, takt::Node::Task& task) override
    {
        auto& t = static_cast<DoubleTask&>(task);
        auto w = out_->acquire_write(t.segment_end);
        w.value() = t.value * 2;
        w.publish();
    }

  private:
    std::shared_ptr<takt::Pipe<int>> in_;
    std::shared_ptr<takt::Pipe<int>> out_;
};
} // namespace

int main()
{
    auto outer_in = std::make_shared<takt::Pipe<int>>("outer-in", 16, 0);
    auto outer_out = std::make_shared<takt::Pipe<int>>("outer-out", 16, 0);
    auto inner_in = std::make_shared<takt::Pipe<int>>("inner-in", 16, 0);
    auto inner_out = std::make_shared<takt::Pipe<int>>("inner-out", 16, 0);

    auto inner = takt::PipelineBuilder::declare("inner");
    inner.add_node("worker");
    inner.add_input_port("worker", "in",
                         {{"pipe_endpoint", takt::make_pipe_endpoint<int>(inner_in)}});
    inner.add_output_port(
        "worker", "out", {{"pipe_endpoint", takt::make_pipe_endpoint<int>(inner_out)}});
    inner.bind_node("worker", std::make_shared<DoubleNode>(inner_in, inner_out));

    auto outer = takt::PipelineBuilder::declare("outer");
    outer.add_node("sg");
    outer.add_input_port("sg", "raw",
                         {{"pipe_endpoint", takt::make_pipe_endpoint<int>(outer_in)}});
    outer.add_output_port(
        "sg", "result", {{"pipe_endpoint", takt::make_pipe_endpoint<int>(outer_out)}});
    outer.bind_subgraph("sg", inner, {{"raw", {"worker", "in"}}},
                        {{"result", {"worker", "out"}}});

    auto runtime = outer.build_runtime();
    runtime->start();

    for (int i = 1; i <= 4; ++i)
    {
        auto w = outer_in->acquire_write(true);
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
