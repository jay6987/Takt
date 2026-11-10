// Checks framework pipe lifecycle wakeups without adapter-owned stop locks.
#include <chrono>
#include <future>
#include <gtest/gtest.h>
#include <takt/takt.h>

namespace
{
class WaitingFrame final : public takt::FrameTaskNode
{
  public:
    std::shared_ptr<takt::Pipe<int>> input =
        std::make_shared<takt::Pipe<int>>("waiting-frame");
    std::promise<void> acquiring;
    // Register the pipe so Takt can interrupt a blocked acquisition.
    WaitingFrame()
        : FrameTaskNode("waiting",
                        3)
    {
        input->set_producer(
            {"source", takt::concurrency(1), takt::frames_per_write(1)}, 0);
        input->set_consumer({"waiting", takt::concurrency(1),
                             takt::frames_per_read(1),
                             takt::overlap_frames(0)});
        input->initialize();
        register_pipe(input);
    }
    // Framework callbacks must finish before the promise and pipe disappear.
    ~WaitingFrame() override
    {
        request_stop();
        join();
    }

  private:
    struct Context final : Frame
    {
        takt::Pipe<int>::ReadToken input;
    };
    // Retain a possible acquired token in the centrally owned frame.
    std::unique_ptr<Frame> create_frame() override
    {
        return std::make_unique<Context>();
    }
    // Signal entry, then block on an empty pipe until stop or failure arrives.
    bool acquire_frame(Frame& frame) override
    {
        acquiring.set_value();
        try
        {
            static_cast<Context&>(frame).input = input->acquire_read();
            return true;
        }
        catch (const takt::PipeClosedAndEmptySignal&)
        {
            return false;
        }
    }
    // No work is admitted by these interrupted acquisitions.
    std::unique_ptr<Task> build_frame_task(Frame&,
                                           uint64_t) override
    {
        ADD_FAILURE() << "interrupted acquisition generated a task";
        return nullptr;
    }
    // No worker should execute after an interrupted acquisition.
    void process_frame_task(size_t,
                            Frame&,
                            Task&) override
    {
        ADD_FAILURE() << "interrupted acquisition reached a worker";
    }
};

// Stop wakes a pipe read as well as the framework's queue/drain waits.
TEST(FrameTaskNode,
     StopWakesBlockedFrameAcquisition)
{
    WaitingFrame node;
    auto ready = node.acquiring.get_future();
    node.start();
    EXPECT_EQ(ready.wait_for(std::chrono::seconds(5)),
              std::future_status::ready);
    node.request_stop();
    node.join();
    EXPECT_EQ(node.failure(), nullptr);
    EXPECT_EQ(node.completed_frames(), 0U);
}

// An explicit upstream fault is not normal EOF and never completes a frame.
TEST(FrameTaskNode,
     FaultedAcquisitionRemainsFailureRatherThanEof)
{
    WaitingFrame node;
    auto ready = node.acquiring.get_future();
    node.start();
    EXPECT_EQ(ready.wait_for(std::chrono::seconds(5)),
              std::future_status::ready);
    node.input->close_with_error("upstream failure");
    node.join();
    ASSERT_NE(node.failure(), nullptr);
    EXPECT_EQ(node.completed_frames(), 0U);
    EXPECT_TRUE(node.input->faulted());
}
} // namespace
