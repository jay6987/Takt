// Verifies configured read sizes and overlap reset at shot boundaries.
#include "pipe_lifecycle_tests_test_support.h"

namespace
{
// Publishes the supplied values as a single shot-ending frame window.
void publish_values(takt::Pipe<int>& pipe,
                    std::initializer_list<int> values)
{
    auto write = pipe.acquire_shot_end_write(values.size());
    ASSERT_EQ(write.frame_count(), values.size());
    size_t index = 0;
    for (const int value : values)
        write.value(index++) = value;
    write.publish();
}

// Checks the first shot retains overlap only in its trailing window.
void verify_initial_overlap_windows(takt::Pipe<int>& pipe)
{
    {
        auto first = pipe.acquire_read();
        EXPECT_EQ(first.frame_count(), 3U);
        EXPECT_EQ(first.overlap_frames(), 0U);
        EXPECT_EQ(first.value(0), 1);
        EXPECT_EQ(first.value(2), 3);
        EXPECT_THROW(first.value(), std::logic_error);
    }
    {
        auto second = pipe.acquire_read();
        EXPECT_EQ(second.frame_count(), 2U);
        EXPECT_EQ(second.overlap_frames(), 1U);
        EXPECT_EQ(second.value(0), 3);
        EXPECT_EQ(second.value(1), 4);
        EXPECT_TRUE(second.is_shot_end());
    }
}

// Checks later shots do not retain overlap from completed shots.
void verify_overlap_resets_after_each_shot(takt::Pipe<int>& pipe)
{
    publish_values(pipe, {5});
    {
        auto single = pipe.acquire_read();
        EXPECT_EQ(single.value(), 5);
    }

    publish_values(pipe, {6, 7});
    auto final = pipe.acquire_read();
    EXPECT_EQ(final.frame_count(), 2U);
    EXPECT_EQ(final.value(0), 6);
    EXPECT_EQ(final.value(1), 7);
}
} // namespace

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
    ASSERT_NO_FATAL_FAILURE(publish_values(pipe, {1, 2, 3, 4}));
    verify_initial_overlap_windows(pipe);
    verify_overlap_resets_after_each_shot(pipe);
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
