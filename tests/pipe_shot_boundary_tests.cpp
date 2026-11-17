// Verifies shot boundary isolation, write flags, and configured shot sizes.
#include "pipe_tests_test_support.h"

// Verifies read windows never combine frames from different shots.
TEST(PipeTests,
     ShotEndPreventsCrossShotRead)
{
    takt::Pipe<int> pipe("shot");
    configure_pipe(pipe, 16, 0, 8, 2, 4);
    pipe.initialize();

    {
        auto w1 = pipe.acquire_shot_end_write(3);
        w1.value(0) = 1;
        w1.value(1) = 2;
        w1.value(2) = 3;
        w1.publish();
    }
    {
        auto w2 = pipe.acquire_shot_end_write(4);
        w2.value(0) = 10;
        w2.value(1) = 11;
        w2.value(2) = 12;
        w2.value(3) = 13;
        w2.publish();
    }

    {
        auto r1 = pipe.acquire_read();
        EXPECT_TRUE(r1.is_shot_start());
        EXPECT_TRUE(r1.is_shot_end());
        EXPECT_EQ(r1.frame_count(), 3U);
        EXPECT_EQ(r1.overlap_frames(), 0U);
        EXPECT_EQ(r1.value(0), 1);
        EXPECT_EQ(r1.value(2), 3);
    }
    {
        auto r2 = pipe.acquire_read();
        EXPECT_TRUE(r2.is_shot_start());
        EXPECT_TRUE(r2.is_shot_end());
        EXPECT_EQ(r2.frame_count(), 4U);
        EXPECT_EQ(r2.overlap_frames(), 0U);
        EXPECT_EQ(r2.value(0), 10);
        EXPECT_EQ(r2.value(3), 13);
    }

    pipe.close();
}

// Verifies write tokens expose the correct shot boundary markers.
TEST(PipeTests,
     ShotFlagsOnWriteTokens)
{
    takt::Pipe<int> pipe("shot-flags");
    configure_pipe(pipe, 8, 0);
    pipe.initialize();

    auto w1 = pipe.acquire_write();
    EXPECT_TRUE(w1.is_shot_start());
    EXPECT_FALSE(w1.is_shot_end());
    w1.value() = 1;
    w1.publish();

    auto w2 = pipe.acquire_shot_end_write(1);
    EXPECT_FALSE(w2.is_shot_start());
    EXPECT_TRUE(w2.is_shot_end());
    w2.value() = 2;
    w2.publish();

    auto w3 = pipe.acquire_shot_end_write(1);
    EXPECT_TRUE(w3.is_shot_start());
    EXPECT_TRUE(w3.is_shot_end());
    w3.value() = 3;
    w3.publish();
}

// Verifies a final write batch respects configuration and terminates a shot.
TEST(PipeTests,
     ShotEndWriteUsesConfiguredSizeAndEndsShot)
{
    takt::Pipe<int> pipe("write-size-contract");
    configure_pipe(pipe, 4, 0, 2, 0, 2);
    pipe.initialize();

    {
        auto write = pipe.acquire_write();
        EXPECT_EQ(write.frame_count(), 2U);
        EXPECT_FALSE(write.is_shot_end());
        write.value(0) = 10;
        write.value(1) = 11;
        write.publish();
    }
    {
        auto write = pipe.acquire_shot_end_write(1);
        EXPECT_EQ(write.frame_count(), 1U);
        EXPECT_TRUE(write.is_shot_end());
        write.value() = 12;
        write.publish();
    }
    EXPECT_THROW(pipe.acquire_shot_end_write(3), std::invalid_argument);

    {
        auto read = pipe.acquire_read();
        EXPECT_EQ(read.frame_count(), 2U);
        EXPECT_FALSE(read.is_shot_end());
        EXPECT_EQ(read.value(0), 10);
        EXPECT_EQ(read.value(1), 11);
    }
    {
        auto read = pipe.acquire_read();
        EXPECT_EQ(read.frame_count(), 1U);
        EXPECT_TRUE(read.is_shot_end());
        EXPECT_EQ(read.value(), 12);
    }
    pipe.close();
}
