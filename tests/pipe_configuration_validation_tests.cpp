// Verifies invalid, duplicate, and cancelled pipe configuration stays rejected.
#include "pipe_lifecycle_tests_test_support.h"

namespace
{
// Checks zero-sized and inconsistent settings fail before initialization.
void reject_invalid_initial_settings(takt::Pipe<int>& pipe)
{
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
}

// Checks duplicate configuration leaves the accepted values unchanged.
void reject_duplicate_settings(takt::Pipe<int>& pipe)
{
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
}

// Checks initialization freezes every pipe configuration surface.
void reject_settings_after_initialization(takt::Pipe<int>& pipe)
{
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
} // namespace

// Verifies rejected settings never overwrite accepted pipe configuration.
TEST(PipeLifecycleTests,
     InvalidAndDuplicateConfigurationDoNotReplaceAcceptedValues)
{
    takt::Pipe<int> pipe("configuration");
    reject_invalid_initial_settings(pipe);
    pipe.set_producer(
        {"producer", takt::concurrency(1), takt::frames_per_write(1)}, 7);
    pipe.set_consumer({"consumer", takt::concurrency(1),
                       takt::frames_per_read(1), takt::overlap_frames(0)});
    reject_duplicate_settings(pipe);
    pipe.initialize();
    reject_settings_after_initialization(pipe);
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
