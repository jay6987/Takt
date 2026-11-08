// Tests explicit pipe configuration, initialization and cancellation rules.
#include "takt/takt.h"

#include <gtest/gtest.h>

#include <limits>
#include <memory>
#include <stdexcept>
#include <type_traits>

namespace
{
struct FrameProbe
{
    static inline int live = 0;
    static inline int copies_until_failure = -1;
    int value = 17;

    // Counts construction so allocation timing can be observed.
    FrameProbe()
    {
        ++live;
    }
    // Moves the probe value while tracking another live instance.
    FrameProbe(FrameProbe&& other) noexcept : value(other.value)
    {
        ++live;
    }
    // Counts copies and injects the configured allocation-time failure.
    FrameProbe(const FrameProbe& other) : value(other.value)
    {
        if (copies_until_failure == 0)
            throw std::runtime_error("frame copy failed");
        if (copies_until_failure > 0)
            --copies_until_failure;
        ++live;
    }
    // Copies the value without changing live-instance accounting.
    FrameProbe& operator=(const FrameProbe&) = default;
    // Decrements the live-instance count on destruction.
    ~FrameProbe()
    {
        --live;
    }
};
} // namespace

// Verifies pipe identity is stable and equal names do not imply shared storage.
TEST(PipeLifecycleTests,
     StableObjectIdentityAndIndependentSameNames)
{
    static_assert(!std::is_copy_constructible_v<takt::Pipe<int>>);
    static_assert(!std::is_move_constructible_v<takt::Pipe<int>>);
    auto first = std::make_shared<takt::Pipe<int>>("same");
    auto second = std::make_shared<takt::Pipe<int>>("same");
    auto endpoint = takt::make_pipe_endpoint(first);
    auto* identity = first.get();
    for (const auto& pipe : {first, second})
    {
        pipe->set_producer(
            {"producer", takt::concurrency(1), takt::frames_per_write(1)}, 0);
        pipe->set_consumer({"consumer", takt::concurrency(1),
                            takt::frames_per_read(1), takt::overlap_frames(0)});
        pipe->initialize();
    }
    EXPECT_EQ(first.get(), identity);
    EXPECT_EQ(first->name(), second->name());
    EXPECT_NE(first.get(), second.get());
    {
        auto write = first->acquire_shot_end_write(1);
        write.value() = 42;
        write.publish();
    }
    EXPECT_EQ(second->queued_frame_count(), 0U);
    {
        auto packet = endpoint->read();
        EXPECT_EQ(std::any_cast<int>(packet.value), 42);
    }
    first->close();
    EXPECT_FALSE(second->closed());
}

// Verifies configuration order does not alter inferred capacity policy.
TEST(PipeLifecycleTests,
     ConfigurationOrderAndExactCapacityPolicies)
{
    for (bool consumer_first : {false, true})
    {
        takt::Pipe<int> pipe("capacity");
        if (consumer_first)
            pipe.set_consumer({"consumer", takt::concurrency(3),
                               takt::frames_per_read(5),
                               takt::overlap_frames(2)});
        pipe.set_producer(
            {"producer", takt::concurrency(2), takt::frames_per_write(4)}, 9);
        if (!consumer_first)
            pipe.set_consumer({"consumer", takt::concurrency(3),
                               takt::frames_per_read(5),
                               takt::overlap_frames(2)});
        EXPECT_EQ(pipe.slot_capacity(), 11U);
        EXPECT_EQ(pipe.producer_config().name, "producer");
        EXPECT_EQ(pipe.consumer_config().overlap_frames, 2U);
        EXPECT_EQ(pipe.frame_template(), 9);
        pipe.request_max_concurrency();
        pipe.request_max_concurrency();
        EXPECT_EQ(pipe.slot_capacity(), 19U);
        pipe.set_slot_capacity(1);
        EXPECT_EQ(pipe.slot_capacity(), 1U);
        pipe.initialize();
        EXPECT_EQ(pipe.slot_capacity(), 1U);
    }
}

// Verifies inferred capacity uses checked arithmetic and honors overrides.
TEST(PipeLifecycleTests,
     CapacityOverflowIsCheckedAndExplicitCapacityBypassesIt)
{
    const auto maximum = std::numeric_limits<size_t>::max();
    takt::Pipe<int> write_overflow("write-overflow");
    write_overflow.set_producer(
        {"producer", takt::concurrency(maximum), takt::frames_per_write(2)}, 0);
    write_overflow.set_consumer({"consumer", takt::concurrency(1),
                                 takt::frames_per_read(1),
                                 takt::overlap_frames(0)});
    EXPECT_THROW(write_overflow.slot_capacity(), std::overflow_error);
    EXPECT_THROW(write_overflow.initialize(), std::overflow_error);
    EXPECT_FALSE(write_overflow.initialized());
    write_overflow.set_slot_capacity(1);
    EXPECT_NO_THROW(write_overflow.initialize());

    takt::Pipe<int> read_overflow("read-overflow");
    read_overflow.set_producer(
        {"producer", takt::concurrency(1), takt::frames_per_write(1)}, 0);
    read_overflow.set_consumer({"consumer", takt::concurrency(maximum),
                                takt::frames_per_read(3),
                                takt::overlap_frames(1)});
    EXPECT_THROW(read_overflow.slot_capacity(), std::overflow_error);

    takt::Pipe<int> read_add_overflow("read-add-overflow");
    read_add_overflow.set_producer(
        {"producer", takt::concurrency(1), takt::frames_per_write(1)}, 0);
    read_add_overflow.set_consumer({"consumer", takt::concurrency(maximum),
                                    takt::frames_per_read(2),
                                    takt::overlap_frames(1)});
    EXPECT_THROW(read_add_overflow.slot_capacity(), std::overflow_error);

    takt::Pipe<int> sum_overflow("sum-overflow");
    sum_overflow.set_producer(
        {"producer", takt::concurrency(maximum), takt::frames_per_write(1)}, 0);
    sum_overflow.set_consumer({"consumer", takt::concurrency(1),
                               takt::frames_per_read(1),
                               takt::overlap_frames(0)});
    EXPECT_EQ(sum_overflow.slot_capacity(), maximum);
    sum_overflow.request_max_concurrency();
    EXPECT_THROW(sum_overflow.slot_capacity(), std::overflow_error);
    sum_overflow.set_slot_capacity(1);
    EXPECT_NO_THROW(sum_overflow.initialize());
}

// Verifies explicit capacity is respected even below nominal batching demand.
TEST(PipeLifecycleTests,
     ExplicitSlotCapacityCanBeSmallerThanConfiguredFrameCounts)
{
    takt::Pipe<int> pipe("small");
    pipe.set_slot_capacity(1);
    pipe.set_producer(
        {"producer", takt::concurrency(8), takt::frames_per_write(4)}, 0);
    pipe.set_consumer({"consumer", takt::concurrency(8),
                       takt::frames_per_read(4), takt::overlap_frames(0)});
    pipe.initialize();
    EXPECT_TRUE(pipe.initialized());
    EXPECT_EQ(pipe.slot_capacity(), 1U);
    EXPECT_THROW(pipe.acquire_write(), std::invalid_argument);
    EXPECT_THROW(pipe.acquire_shot_end_write(2), std::invalid_argument);
    EXPECT_THROW(pipe.acquire_read(), std::invalid_argument);
    {
        auto write = pipe.acquire_shot_end_write(1);
        write.value() = 7;
        write.publish();
    }
    EXPECT_THROW(pipe.acquire_read(), std::invalid_argument);
}

// Verifies allocation occurs at initialization with rollback on copy failure.
TEST(PipeLifecycleTests,
     InitializationAllocatesFrameSlotsAndHasStrongFailureGuarantee)
{
    ASSERT_EQ(FrameProbe::live, 0);
    FrameProbe::copies_until_failure = -1;
    {
        takt::Pipe<FrameProbe> pipe("allocation");
        EXPECT_EQ(FrameProbe::live, 0);
        pipe.set_producer(
            {"producer", takt::concurrency(1), takt::frames_per_write(1)},
            FrameProbe{});
        pipe.set_consumer({"consumer", takt::concurrency(1),
                           takt::frames_per_read(1), takt::overlap_frames(0)});
        pipe.set_slot_capacity(4);
        EXPECT_EQ(FrameProbe::live, 1);
        FrameProbe::copies_until_failure = 2;
        EXPECT_THROW(pipe.initialize(), std::runtime_error);
        EXPECT_FALSE(pipe.initialized());
        EXPECT_FALSE(pipe.closed());
        EXPECT_FALSE(pipe.faulted());
        EXPECT_EQ(pipe.queued_frame_count(), 0U);
        EXPECT_EQ(FrameProbe::live, 1);
        EXPECT_EQ(pipe.frame_template().value, 17);
        EXPECT_THROW(pipe.acquire_write(), std::logic_error);
        pipe.set_slot_capacity(3);
        FrameProbe::copies_until_failure = -1;
        pipe.initialize();
        EXPECT_TRUE(pipe.initialized());
        EXPECT_EQ(FrameProbe::live, 3);
        EXPECT_THROW(pipe.frame_template(), std::logic_error);
        auto write = pipe.acquire_write();
        EXPECT_EQ(write.value().value, 17);
        write.publish();
    }
    EXPECT_EQ(FrameProbe::live, 0);
}

// Verifies incomplete configuration and premature token acquisition fail.
TEST(PipeLifecycleTests,
     MissingConfigurationAndAllUninitializedAcquisitionsFail)
{
    takt::Pipe<int> pipe("missing");
    EXPECT_EQ(pipe.queued_frame_count(), 0U);
    EXPECT_FALSE(pipe.initialized());
    EXPECT_FALSE(pipe.closed());
    EXPECT_FALSE(pipe.faulted());
    EXPECT_TRUE(pipe.fault_message().empty());
    EXPECT_THROW(pipe.slot_capacity(), std::logic_error);
    EXPECT_THROW(pipe.producer_config(), std::logic_error);
    EXPECT_THROW(pipe.consumer_config(), std::logic_error);
    EXPECT_THROW(pipe.frame_template(), std::logic_error);
    EXPECT_THROW(pipe.initialize(), std::logic_error);
    EXPECT_THROW(pipe.acquire_write(), std::logic_error);
    EXPECT_THROW(pipe.acquire_shot_end_write(1), std::logic_error);
    EXPECT_THROW(pipe.acquire_read(), std::logic_error);
    pipe.set_consumer({"consumer", takt::concurrency(1),
                       takt::frames_per_read(1), takt::overlap_frames(0)});
    EXPECT_THROW(pipe.initialize(), std::logic_error);
    pipe.set_producer(
        {"producer", takt::concurrency(1), takt::frames_per_write(1)}, 0);
    EXPECT_NO_THROW(pipe.initialize());
}

// Verifies rejected settings never overwrite accepted pipe configuration.
TEST(PipeLifecycleTests,
     InvalidAndDuplicateConfigurationDoNotReplaceAcceptedValues)
{
    takt::Pipe<int> pipe("configuration");
    EXPECT_THROW(pipe.set_slot_capacity(0), std::invalid_argument);
    EXPECT_THROW(
        pipe.set_producer(
            {"producer", takt::concurrency(0), takt::frames_per_write(1)}, 1),
        std::invalid_argument);
    EXPECT_THROW(
        pipe.set_producer(
            {"producer", takt::concurrency(1), takt::frames_per_write(0)}, 1),
        std::invalid_argument);
    EXPECT_THROW(
        pipe.set_consumer({"consumer", takt::concurrency(0),
                           takt::frames_per_read(1), takt::overlap_frames(0)}),
        std::invalid_argument);
    EXPECT_THROW(
        pipe.set_consumer({"consumer", takt::concurrency(1),
                           takt::frames_per_read(0), takt::overlap_frames(0)}),
        std::invalid_argument);
    EXPECT_THROW(
        pipe.set_consumer({"consumer", takt::concurrency(1),
                           takt::frames_per_read(2), takt::overlap_frames(2)}),
        std::invalid_argument);
    pipe.set_producer(
        {"producer", takt::concurrency(1), takt::frames_per_write(1)}, 7);
    pipe.set_consumer({"consumer", takt::concurrency(1),
                       takt::frames_per_read(1), takt::overlap_frames(0)});
    EXPECT_THROW(pipe.set_producer({"replacement", takt::concurrency(2),
                                    takt::frames_per_write(2)},
                                   99),
                 std::logic_error);
    EXPECT_THROW(
        pipe.set_consumer({"replacement", takt::concurrency(2),
                           takt::frames_per_read(2), takt::overlap_frames(0)}),
        std::logic_error);
    EXPECT_EQ(pipe.frame_template(), 7);
    EXPECT_EQ(pipe.producer_config().name, "producer");
    EXPECT_EQ(pipe.consumer_config().name, "consumer");
    pipe.initialize();
    EXPECT_THROW(pipe.initialize(), std::logic_error);
    EXPECT_THROW(
        pipe.set_producer(
            {"producer", takt::concurrency(1), takt::frames_per_write(1)}, 0),
        std::logic_error);
    EXPECT_THROW(
        pipe.set_consumer({"consumer", takt::concurrency(1),
                           takt::frames_per_read(1), takt::overlap_frames(0)}),
        std::logic_error);
    EXPECT_THROW(pipe.set_slot_capacity(1), std::logic_error);
    EXPECT_THROW(pipe.request_max_concurrency(), std::logic_error);
    EXPECT_THROW(pipe.set_descriptor({}), std::logic_error);
}

// Verifies cancellation freezes configuration without allocating frames.
TEST(PipeLifecycleTests,
     ConfigurationCancellationDoesNotAllocateOrPermitInitialization)
{
    ASSERT_EQ(FrameProbe::live, 0);
    {
        takt::Pipe<FrameProbe> pipe("cancelled");
        pipe.close();
        pipe.close();
        EXPECT_TRUE(pipe.closed());
        EXPECT_FALSE(pipe.faulted());
        EXPECT_FALSE(pipe.initialized());
        EXPECT_EQ(FrameProbe::live, 0);
        EXPECT_THROW(pipe.initialize(), std::logic_error);
        EXPECT_THROW(pipe.set_consumer({"consumer", takt::concurrency(1),
                                        takt::frames_per_read(1),
                                        takt::overlap_frames(0)}),
                     std::logic_error);
        EXPECT_THROW(pipe.set_producer({"producer", takt::concurrency(1),
                                        takt::frames_per_write(1)},
                                       FrameProbe{}),
                     std::logic_error);
        EXPECT_THROW(pipe.set_slot_capacity(1), std::logic_error);
        EXPECT_THROW(pipe.request_max_concurrency(), std::logic_error);
        EXPECT_THROW(pipe.set_descriptor({}), std::logic_error);
        EXPECT_THROW(pipe.acquire_read(), std::logic_error);
    }
    EXPECT_EQ(FrameProbe::live, 0);
    takt::Pipe<int> failed("failed-configuration");
    EXPECT_THROW(failed.close_with_error(""), std::invalid_argument);
    EXPECT_FALSE(failed.closed());
    failed.close_with_error("first failure");
    failed.close_with_error("second failure");
    EXPECT_TRUE(failed.closed());
    EXPECT_TRUE(failed.faulted());
    EXPECT_EQ(failed.fault_message(), "first failure");
    EXPECT_THROW(failed.initialize(), std::logic_error);
}

// Verifies overlap needs no separate read-history configuration.
TEST(PipeLifecycleTests,
     ConfiguredOverlapInitializesWithoutHistoryPolicy)
{
    takt::Pipe<int> pipe("overlap");
    pipe.set_producer(
        {"producer", takt::concurrency(1), takt::frames_per_write(1)}, 0);
    pipe.set_consumer({"consumer", takt::concurrency(1),
                       takt::frames_per_read(3), takt::overlap_frames(1)});
    EXPECT_NO_THROW(pipe.initialize());
}

// Verifies read windows use configured overlap and reset it between shots.
TEST(PipeLifecycleTests,
     AcquireReadUsesConfiguredWindowAndResetsOverlapAtShotEnd)
{
    takt::Pipe<int> pipe("configured-windows");
    pipe.set_producer(
        {"producer", takt::concurrency(1), takt::frames_per_write(4)}, 0);
    pipe.set_consumer({"consumer", takt::concurrency(1),
                       takt::frames_per_read(3), takt::overlap_frames(1)});
    pipe.initialize();
    {
        auto write = pipe.acquire_shot_end_write(4);
        ASSERT_EQ(write.frame_count(), 4U);
        for (size_t i = 0; i < write.frame_count(); ++i)
            write.value(i) = static_cast<int>(i + 1);
        write.publish();
    }
    {
        auto read = pipe.acquire_read();
        EXPECT_EQ(read.frame_count(), 3U);
        EXPECT_EQ(read.overlap_frames(), 0U);
        EXPECT_EQ(read.value(0), 1);
        EXPECT_EQ(read.value(2), 3);
        EXPECT_THROW(read.value(), std::logic_error);
    }
    {
        auto read = pipe.acquire_read();
        EXPECT_EQ(read.frame_count(), 2U);
        EXPECT_EQ(read.overlap_frames(), 1U);
        EXPECT_EQ(read.value(0), 3);
        EXPECT_EQ(read.value(1), 4);
        EXPECT_TRUE(read.is_shot_end());
    }
    {
        auto write = pipe.acquire_shot_end_write(1);
        write.value() = 5;
        write.publish();
    }
    {
        auto read = pipe.acquire_read();
        EXPECT_EQ(read.value(), 5);
    }
    {
        auto write = pipe.acquire_shot_end_write(2);
        EXPECT_EQ(write.frame_count(), 2U);
        write.value(0) = 6;
        write.value(1) = 7;
        write.publish();
    }
    auto read = pipe.acquire_read();
    EXPECT_EQ(read.frame_count(), 2U);
    EXPECT_EQ(read.value(0), 6);
    EXPECT_EQ(read.value(1), 7);
}

// Verifies nonoverlapping windows consume the configured frame count.
TEST(PipeLifecycleTests,
     AcquireReadUsesConfiguredSizeWithoutOverlap)
{
    takt::Pipe<int> pipe("configured-read");
    pipe.set_producer(
        {"producer", takt::concurrency(1), takt::frames_per_write(4)}, 0);
    pipe.set_consumer({"consumer", takt::concurrency(1),
                       takt::frames_per_read(2), takt::overlap_frames(0)});
    pipe.initialize();
    {
        auto write = pipe.acquire_shot_end_write(4);
        for (size_t i = 0; i < write.frame_count(); ++i)
            write.value(i) = static_cast<int>(i);
        write.publish();
    }
    for (int first : {0, 2})
    {
        auto read = pipe.acquire_read();
        EXPECT_EQ(read.frame_count(), 2U);
        EXPECT_EQ(read.value(0), first);
        EXPECT_EQ(read.value(1), first + 1);
    }
}
