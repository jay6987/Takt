// Implements grouped-task admission, processing, and barrier completion probes.
#include "node_barrier_tests_test_support.h"

#include <gtest/gtest.h>

#include <stdexcept>

namespace takt_test
{
// Injects the original failure at the configured execution phase.
void GroupProbe::maybe_fail(int phase)
{
    if (fail == phase)
    {
        throw std::domain_error("original group failure");
    }
}

// Starts the next group only after the previous payloads have drained.
bool GroupProbe::start_group()
{
    if (ended.load() == 2)
    {
        return false;
    }
    EXPECT_EQ(live_items.load(), 0U);
    ingress = std::this_thread::get_id();
    context_ = std::make_unique<Context>(*this);
    next_item_ = 0;
    ++acquired;
    maybe_fail(0);
    maybe_fail(1);
    return true;
}

// Builds tracked tasks and marks the last task as a group barrier.
std::unique_ptr<takt::Node::Task> GroupProbe::build_task(uint64_t)
{
    if (!context_ && !start_group())
    {
        return nullptr;
    }
    if (generating)
    {
        generating(next_item_);
    }
    if (next_item_ == 3)
    {
        maybe_fail(2);
    }
    const bool empty = count == 0;
    const bool last = empty || next_item_ + 1 == count;
    auto item = std::make_unique<Item>(*this, *context_, next_item_, last);
    if (!empty)
    {
        ++next_item_;
        ++generated;
    }
    return item;
}

// Runs optional test work and verifies ingress/worker separation.
void GroupProbe::process_task(size_t worker,
                              takt::Node::Task& task)
{
    EXPECT_NE(ingress, std::this_thread::get_id());
    auto& item = static_cast<Item&>(task);
    EXPECT_EQ(item.context, context_.get());
    if (count == 0)
    {
        return;
    }
    maybe_fail(3);
    if (work)
    {
        work(worker, task);
    }
    ++processed;
}

// Releases group storage after all admitted tasks are destroyed.
void GroupProbe::on_barrier_complete(uint64_t)
{
    EXPECT_EQ(live_items.load(), 0U);
    EXPECT_EQ(ingress, std::this_thread::get_id());
    maybe_fail(4);
    ++ended;
    context_.reset();
}

// Drains retained context and normally closes the output pipe.
void GroupProbe::on_ingress_manager_done()
{
    context_.reset();
    if (!pipe->closed() && !pipe->faulted())
    {
        pipe->close();
    }
}

// Faults the output and optionally injects a secondary observer failure.
void GroupProbe::on_exception(const takt::NodeExceptionContext& exception)
{
    if (!pipe->faulted())
    {
        pipe->close_with_error(exception.message);
    }
    Node::on_exception(exception);
    if (throw_failure_observer)
    {
        throw std::runtime_error("secondary observer failure");
    }
}
} // namespace takt_test
