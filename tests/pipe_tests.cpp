// Verifies pipe frame windows and mutable read-token access.
#include "pipe_tests_test_support.h"

// Verifies single-frame publication and consumption.
// Verifies single-frame publication and consumption.
TEST(PipeTests,
     SingleElementReadWrite)
{
    takt::set_debug_enabled(true);
    takt::debug("smoke test debug log");

    takt::Pipe<int> pipe("numbers");
    configure_pipe(pipe, 2, 0);
    pipe.initialize();
    {
        auto wt = pipe.acquire_shot_end_write(1);
        wt.value() = 42;
        wt.publish();
    }
    {
        auto rt = pipe.acquire_read();
        EXPECT_EQ(rt.value(), 42);
    }
    pipe.close();
}

// Verifies batched writes are exposed through read windows.
TEST(PipeTests,
     FrameWindowReadWrite)
{
    takt::Pipe<int> window_pipe("window");
    configure_pipe(window_pipe, 8, 0, 4, 0, 4);
    window_pipe.initialize();
    {
        auto wb = window_pipe.acquire_shot_end_write(4);
        wb.value(0) = 10;
        wb.value(1) = 20;
        wb.value(2) = 30;
        wb.value(3) = 40;
        wb.publish();
    }
    {
        auto rb = window_pipe.acquire_read();
        EXPECT_EQ(rb.frame_count(), 4U);
        EXPECT_EQ(rb.value(0), 10);
        EXPECT_EQ(rb.value(3), 40);
    }
    window_pipe.close();
}

// Verifies writable access to a single readable frame.
TEST(PipeTests,
     MutableReadTokenSingleElementInplace)
{
    takt::Pipe<int> pipe("mutable-single");
    configure_pipe(pipe, 4, 0);
    pipe.initialize();
    {
        auto w = pipe.acquire_shot_end_write(1);
        w.value() = 11;
        w.publish();
    }

    {
        auto r = pipe.acquire_read();
        r.mutable_value() *= 3;
    }

    {
        auto w = pipe.acquire_shot_end_write(1);
        w.value() = 7;
        w.publish();
    }

    {
        auto r = pipe.acquire_read();
        EXPECT_EQ(r.value(), 7);
    }
    pipe.close();
}

// Verifies in-place mutation preserves the end-of-shot boundary.
TEST(PipeTests,
     MutableReadTokenInPlaceBeforeShotEnd)
{
    takt::Pipe<int> pipe("mutable-window");
    configure_pipe(pipe, 8, 0, 4, 0, 4);
    pipe.initialize();
    {
        auto w = pipe.acquire_write();
        w.value(0) = 1;
        w.value(1) = 2;
        w.value(2) = 3;
        w.value(3) = 4;
        w.publish();
    }

    {
        auto r = pipe.acquire_read();
        EXPECT_FALSE(r.is_shot_end());
        for (size_t i = 0; i < r.frame_count(); ++i)
        {
            r.mutable_value(i) += 10;
        }
    }

    {
        auto w = pipe.acquire_shot_end_write(2);
        w.value(0) = 9;
        w.value(1) = 8;
        w.publish();
    }

    {
        auto r = pipe.acquire_read();
        EXPECT_EQ(r.value(0), 9);
        EXPECT_EQ(r.value(1), 8);
    }
    pipe.close();
}
