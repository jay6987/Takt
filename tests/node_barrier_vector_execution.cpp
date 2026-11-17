// Implements shared-vector task admission and synchronized barrier publication.
#include "node_barrier_tests_test_support.h"

#include <gtest/gtest.h>

#include <stdexcept>
#include <utility>

namespace takt_test
{
// Acquires one shared frame and admits disjoint region tasks lazily.
std::unique_ptr<takt::Node::Task> VectorBarrierProbe::build_task(uint64_t)
{
    if (!context_)
    {
        if (acquired.load() != published.load() ||
            acquired.load() != released.load())
        {
            ordering_valid = false;
        }
        try
        {
            auto read = input->acquire_read();
            context_ = std::make_unique<Context>(*this, acquired.load());
            context_->read = std::move(read);
            context_->write = output->acquire_shot_end_write(1);
        }
        catch (const takt::PipeClosedAndEmptySignal&)
        {
            context_.reset();
            output->close();
            return nullptr;
        }
        ++acquired;
        next_region_ = 0;
    }

    const auto size = context_->read.value(0).size();
    const auto first = size * next_region_ / worker_count();
    const auto last = size * (next_region_ + 1) / worker_count();
    const bool barrier = next_region_ + 1 == worker_count();
    auto task =
        std::make_unique<Region>(*this, *context_, first, last, barrier);
    ++next_region_;
    return task;
}

// Waits for the test gate before transforming this worker's vector interval.
void VectorBarrierProbe::process_task(size_t worker,
                                      takt::Node::Task& base)
{
    auto& task = static_cast<Region&>(base);
    const auto group = task.context->group;
    {
        std::unique_lock<std::mutex> lock(gate_mutex);
        entered[group][worker] = true;
        gate_changed.notify_all();
        if (!gate_changed.wait_for(lock, std::chrono::seconds(5),
                                   [&] { return release_group[group]; }))
        {
            throw std::runtime_error("region release timeout");
        }
    }

    const auto& source = task.context->read.value(0);
    auto& destination = task.context->write.value(0);
    for (size_t i = task.first; i < task.last; ++i)
    {
        destination[i] = source[i] + 100.0F * group;
    }
}

// Publishes the completed vector after every region payload has drained.
void VectorBarrierProbe::on_barrier_complete(uint64_t)
{
    EXPECT_EQ(live_regions.load(), 0U);
    context_->write.publish();
    ++published;
    context_.reset();
}

// Releases any retained frame context when admission finishes.
void VectorBarrierProbe::on_ingress_manager_done()
{
    context_.reset();
}

// Propagates processing failure through both vector boundary pipes.
void VectorBarrierProbe::on_exception(
    const takt::NodeExceptionContext& exception)
{
    input->close_with_error(exception.message);
    output->close_with_error(exception.message);
    Node::on_exception(exception);
}

// Cancels admission and wakes operations blocked on either boundary.
void VectorBarrierProbe::request_stop() noexcept
{
    Node::request_stop();
    close_pipe(input);
    close_pipe(output);
}
} // namespace takt_test
