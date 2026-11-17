// Verifies bounded pipe blocking, publication wakeups, and idempotent closure.
#include "pipe_tests_test_support.h"

// Verifies producer backpressure is released by consumer token destruction.
TEST(PipeTests,
     WriteBlocksWhenFullUntilReadReleasesSpace)
{
    using namespace std::chrono_literals;

    takt::Pipe<int> pipe("write-blocking");
    configure_pipe(pipe, 2, 0, 1, 0, 2);
    pipe.initialize();
    {
        auto w = pipe.acquire_shot_end_write(2);
        w.value(0) = 1;
        w.value(1) = 2;
        w.publish();
    }

    auto writer = std::async(std::launch::async,
                             [&pipe]
                             {
                                 auto w = pipe.acquire_shot_end_write(1);
                                 w.value() = 3;
                                 w.publish();
                             });

    EXPECT_EQ(writer.wait_for(100ms), std::future_status::timeout);

    {
        auto r = pipe.acquire_read();
        EXPECT_EQ(r.value(), 1);
    }

    EXPECT_EQ(writer.wait_for(500ms), std::future_status::ready);
    pipe.close();
}

// Verifies a waiting reader wakes only after frames are published.
TEST(PipeTests,
     ReadBlocksWhenEmptyUntilWritePublishes)
{
    using namespace std::chrono_literals;

    takt::Pipe<int> pipe("read-blocking");
    configure_pipe(pipe, 2, 0);
    pipe.initialize();

    auto reader = std::async(std::launch::async,
                             [&pipe]
                             {
                                 auto r = pipe.acquire_read();
                                 return r.value();
                             });

    EXPECT_EQ(reader.wait_for(100ms), std::future_status::timeout);

    {
        auto w = pipe.acquire_shot_end_write(1);
        w.value() = 99;
        w.publish();
    }

    EXPECT_EQ(reader.wait_for(500ms), std::future_status::ready);
    EXPECT_EQ(reader.get(), 99);
    pipe.close();
}

// Verifies repeated reads of a drained closed pipe signal completion.
TEST(PipeTests,
     CloseReadSignalIsIdempotent)
{
    takt::Pipe<int> pipe("close-idempotent");
    configure_pipe(pipe, 2, 0);
    pipe.initialize();
    pipe.close();

    for (int i = 0; i < 3; ++i)
    {
        EXPECT_THROW(pipe.acquire_read(), takt::PipeClosedAndEmptySignal);
    }
}
