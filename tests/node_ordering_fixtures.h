// Supplies ingress reordering and ordered emission nodes for pipeline tests.
#pragma once
#include "node_diagnostic_fixtures.h"
#include "node_tests_test_support_common.h"
namespace
{

/// Represents the ingress reorder agent test fixture.
class IngressReorderAgent final : public takt::Node
{
  public:
    // Binds unordered indexed input to the synchronized ordered store.
    IngressReorderAgent(takt::Pipe<IndexedChar>& in,
                        OrderedCharStore& store)
        : takt::Node("ingress-reorder"),
          in_(in),
          store_(store)
    {
    }

  protected:
    /// Represents the worker task test fixture.
    struct WorkerTask final : public takt::Node::Task
    {
    };

    // Admits one persistent task that drains indexed input.
    std::unique_ptr<takt::Node::Task> build_task(uint64_t sequence_id) override
    {
        if (sequence_id >= worker_count())
        {
            return nullptr;
        }
        return std::make_unique<WorkerTask>();
    }

    // Stores each indexed character and signals completion after input drains.
    void process_task(size_t,
                      takt::Node::Task&) override
    {
        try
        {
            while (true)
            {
                auto r = in_.acquire_read();
                store_.put(r.value().index, r.value().value);
            }
        }
        catch (const takt::PipeClosedAndEmptySignal&)
        {
            store_.mark_done();
        }
    }

  private:
    takt::Pipe<IndexedChar>& in_;
    OrderedCharStore& store_;
};

/// Represents the ordered emit agent test fixture.
class OrderedEmitAgent final : public takt::Node
{
  public:
    // Binds the ordered character store to a frame output pipe.
    OrderedEmitAgent(OrderedCharStore& store,
                     takt::Pipe<char>& out)
        : takt::Node("ordered-emit"),
          store_(store),
          out_(out)
    {
    }

  protected:
    /// Represents the worker task test fixture.
    struct WorkerTask final : public takt::Node::Task
    {
    };

    // Admits one persistent task responsible for ordered output.
    std::unique_ptr<takt::Node::Task> build_task(uint64_t sequence_id) override
    {
        if (sequence_id >= worker_count())
        {
            return nullptr;
        }
        return std::make_unique<WorkerTask>();
    }

    // Publishes consecutive characters and closes output when the store drains.
    void process_task(size_t,
                      takt::Node::Task&) override
    {
        size_t expected = 0;
        while (true)
        {
            char c = '\0';
            if (!store_.wait_pop(expected, c))
            {
                out_.close();
                break;
            }
            auto w = out_.acquire_shot_end_write(1);
            w.value() = c;
            w.publish();
            ++expected;
        }
    }

  private:
    OrderedCharStore& store_;
    takt::Pipe<char>& out_;
};
} // namespace
