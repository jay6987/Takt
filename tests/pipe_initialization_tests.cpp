// Verifies pipe storage allocation, explicit capacities, and initialization.
#include "pipe_lifecycle_tests_test_support.h"

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
