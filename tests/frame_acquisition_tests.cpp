// Checks pipe lifecycle wakeups for ordinary Nodes that acquire in ingress.
#include <chrono>
#include <future>
#include <gtest/gtest.h>
#include <takt/takt.h>

namespace
{
/// Waits in ingress until test cancellation closes the observed pipe.
class WaitingNode final : public takt::Node
{
  public:
    std::shared_ptr<takt::Pipe<int>> input =
        std::make_shared<takt::Pipe<int>>("waiting-node");
    std::promise<void> acquiring;

    // Configure the input before ingress blocks on its first task request.
    WaitingNode()
        : Node("waiting",
               3)
    {
        input->set_producer(
            {"source", takt::concurrency(1), takt::frames_per_write(1)}, 0);
        input->set_consumer({"waiting", takt::concurrency(1),
                             takt::frames_per_read(1),
                             takt::overlap_frames(0)});
        input->initialize();
    }

    // Stop callbacks before destroying their pipe and promise.
    ~WaitingNode() override
    {
        request_stop();
        join();
    }

  protected:
    /// Retains a read token while ingress is blocked on the input pipe.
    struct ReadTask final : Task
    {
        takt::Pipe<int>::ReadToken token;
    };

  private:
    // Block on the input pipe and retain any acquired token in the task.
    std::unique_ptr<Task> build_task(uint64_t) override
    {
        acquiring.set_value();
        auto task = std::make_unique<ReadTask>();
        try
        {
            task->token = input->acquire_read();
            return task;
        }
        catch (const takt::PipeClosedAndEmptySignal&)
        {
            return nullptr;
        }
    }

    // No worker should run after acquisition is interrupted.
    void process_task(size_t,
                      Task&) override
    {
        ADD_FAILURE() << "interrupted acquisition reached a worker";
    }

    // Fault-close the owned input while preserving the original exception.
    void on_exception(const takt::NodeExceptionContext& context) override
    {
        if (!input->faulted())
            input->close_with_error(context.message);
        Node::on_exception(context);
    }

  public:
    // Node cancellation alone does not interrupt an external pipe wait.
    void request_stop() noexcept override
    {
        Node::request_stop();
        if (input && !input->closed() && !input->faulted())
        {
            try
            {
                input->close();
            }
            catch (...)
            {
            }
        }
    }
};

// Derived stop closes the pipe so a blocked ingress read wakes promptly.
TEST(NodeBarrier,
     StopWakesBlockedTaskAcquisition)
{
    WaitingNode node;
    auto ready = node.acquiring.get_future();
    node.start();
    EXPECT_EQ(ready.wait_for(std::chrono::seconds(5)),
              std::future_status::ready);
    node.request_stop();
    node.join();
    EXPECT_EQ(node.failure(), nullptr);
}

// An upstream fault remains an error rather than normal end-of-stream.
TEST(NodeBarrier,
     FaultedTaskAcquisitionRemainsFailureRatherThanEof)
{
    WaitingNode node;
    auto ready = node.acquiring.get_future();
    node.start();
    EXPECT_EQ(ready.wait_for(std::chrono::seconds(5)),
              std::future_status::ready);
    node.input->close_with_error("upstream failure");
    node.join();
    ASSERT_NE(node.failure(), nullptr);
    EXPECT_TRUE(node.input->faulted());
}
} // namespace
