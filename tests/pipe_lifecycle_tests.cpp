// Verifies pipe identity, configuration ordering, and capacity policies.
#include "pipe_lifecycle_tests_test_support.h"

// Verifies pipe identity is stable and equal names do not imply shared storage.
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
