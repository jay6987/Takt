// Verifies overlapping read windows and slot retention by active read tokens.
#include "pipe_tests_test_support.h"

// Verifies consecutive read windows retain the configured overlap.
TEST(PipeTests,
     OverlapRead)
{
    takt::Pipe<int> window_pipe("window-overlap");
    configure_pipe(window_pipe, 8, 0, 4, 2, 4);
    window_pipe.initialize();
    {
        auto first_write = window_pipe.acquire_write();
        for (size_t i = 0; i < first_write.frame_count(); ++i)
        {
            first_write.value(i) = static_cast<int>(i + 1); // 1..4
        }
        first_write.publish();
    }
    {
        auto last_write = window_pipe.acquire_shot_end_write(2);
        last_write.value(0) = 5;
        last_write.value(1) = 6;
        last_write.publish();
    }
    auto first = window_pipe.acquire_read(); // 1,2,3,4
    EXPECT_EQ(first.value(0), 1);
    EXPECT_EQ(first.value(3), 4);
    auto second = window_pipe.acquire_read(); // 3,4,5,6
    EXPECT_EQ(second.overlap_frames(), 2U);
    EXPECT_EQ(second.value(0), 3);
    EXPECT_EQ(second.value(1), 4);
    EXPECT_EQ(second.value(2), 5);
    EXPECT_EQ(second.value(3), 6);
    EXPECT_EQ(&first.value(2), &second.value(0));
    EXPECT_EQ(&first.value(3), &second.value(1));
    window_pipe.close();
}

// Verifies live read tokens prevent reuse of their frame slots.
TEST(PipeTests,
     ActiveReadTokensPinSlotsUntilReleased)
{
    takt::Pipe<int> pipe("overlap-token-pinning");
    configure_pipe(pipe, 3, 0, 2, 1, 2);
    pipe.initialize();
    {
        auto write = pipe.acquire_write();
        write.value(0) = 1;
        write.value(1) = 2;
        write.publish();
    }
    {
        auto write = pipe.acquire_shot_end_write(1);
        write.value() = 3;
        write.publish();
    }

    auto first = pipe.acquire_read();
    auto second = pipe.acquire_read();
    ASSERT_EQ(second.overlap_frames(), 1U);
    EXPECT_EQ(&first.value(1), &second.value(0));

    auto writer = std::async(std::launch::async,
                             [&pipe]
                             {
                                 auto write = pipe.acquire_shot_end_write(1);
                                 write.value() = 4;
                                 write.publish();
                             });
    EXPECT_EQ(writer.wait_for(std::chrono::milliseconds(50)),
              std::future_status::timeout);

    first = {};
    EXPECT_EQ(writer.wait_for(std::chrono::seconds(2)),
              std::future_status::ready);
    writer.get();
    EXPECT_EQ(second.value(0), 2);
    EXPECT_EQ(second.value(1), 3);

    pipe.close();
    auto third = pipe.acquire_read();
    ASSERT_EQ(third.frame_count(), 1U);
    EXPECT_EQ(third.overlap_frames(), 0U);
    EXPECT_EQ(third.value(), 4);
}
