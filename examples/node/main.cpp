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
    auto in = std::make_shared<takt::Pipe<int>>("node-in", 16, 0);
    auto out = std::make_shared<takt::Pipe<int>>("node-out", 16, 0);

    DoubleNode node(in, out);
    node.set_stage_name("double-stage");
    node.add_input_port_name("raw");
    node.add_output_port_name("result");

    node.start();
    for (int i = 1; i <= 4; ++i)
    {
        auto w = in->acquire_write(true);
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
