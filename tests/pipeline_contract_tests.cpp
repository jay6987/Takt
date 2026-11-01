#include "takt/takt.h"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <future>
#include <memory>
#include <optional>
#include <stdexcept>
#include <thread>
#include <vector>

namespace
{
using namespace std::chrono_literals;
using Frame = std::vector<float>;
using FramePipe = takt::Pipe<Frame>;

struct CopyProbe
{
    static inline int copies = 0;
    CopyProbe() = default;
    CopyProbe(const CopyProbe&)
    {
        ++copies;
    }
    CopyProbe& operator=(const CopyProbe&)
    {
        ++copies;
        return *this;
    }
};

class FrameNode final : public takt::Node
{
  public:
    FrameNode(FramePipe& input, FramePipe& output, bool inject_failure)
        : Node("frame", 1, 1), input_(input), output_(output),
          inject_failure_(inject_failure)
    {
    }
    ~FrameNode() override
    {
        request_stop();
        input_.close();
        output_.close();
        join();
    }
    std::atomic<bool> storage_preserved{true};

  private:
    struct FrameTask final : Task
    {
        FrameTask(FramePipe::ReadToken input_token, FramePipe::WriteToken output_token)
            : input(std::move(input_token)), output(std::move(output_token)),
              reserved_address(input.value().data())
        {
        }
        FramePipe::ReadToken input;
        FramePipe::WriteToken output;
        const float* reserved_address;
    };
    std::unique_ptr<Task> build_task(uint64_t) override
    {
        try
        {
            auto input = input_.acquire_read();
            auto output = output_.acquire_write(input.is_segment_end());
            return std::make_unique<FrameTask>(std::move(input), std::move(output));
        }
        catch (const takt::PipeClosedAndEmptySignal&)
        {
            return nullptr;
        }
    }
    void process_task(size_t, Task& task) override
    {
        auto& frame = static_cast<FrameTask&>(task);
        if (inject_failure_ && task.sequence_id == 2)
        {
            throw std::runtime_error("injected frame failure");
        }
        storage_preserved.store(storage_preserved.load() &&
                                frame.input.value().data() == frame.reserved_address);
        for (size_t i = 0; i < frame.input.value().size(); ++i)
        {
            frame.output.value()[i] = frame.input.value()[i] * 2.0F;
        }
        frame.output.publish();
    }
    void on_ingress_manager_done() override
    {
        output_.close();
    }
    void on_exception(const takt::NodeExceptionContext& context) override
    {
        input_.close_with_error(context.message);
        output_.close_with_error(context.message);
        Node::on_exception(context);
    }
    FramePipe& input_;
    FramePipe& output_;
    bool inject_failure_;
};

class BlockedNode final : public takt::Node
{
  public:
    explicit BlockedNode(std::shared_future<void> release)
        : Node("bounded", 1, 1), release_(std::move(release))
    {
    }
    std::atomic<int> built{0};
    std::promise<void> second_built;

  private:
    std::unique_ptr<Task> build_task(uint64_t sequence) override
    {
        if (sequence == 4)
            return nullptr;
        auto task = std::make_unique<Task>();
        if (built.fetch_add(1) == 1)
            second_built.set_value();
        return task;
    }
    void process_task(size_t, Task&) override
    {
        release_.wait();
    }
    std::shared_future<void> release_;
};
} // namespace

TEST(PipelineContracts, DisabledHistoryDoesNotCopyPayloadAtReadRelease)
{
    takt::Pipe<CopyProbe> pipe("no-history", 1, {}, {},
                               takt::ReadHistoryPolicy::Disabled);
    CopyProbe::copies = 0;
    {
        auto write = pipe.acquire_write();
        write.publish();
    }
    {
        auto read = pipe.acquire_read();
        auto retained = std::move(read);
        (void)retained.value();
    }
    EXPECT_EQ(CopyProbe::copies, 0);
    EXPECT_THROW((void)pipe.acquire_read_batch(1, 1), std::invalid_argument);
    pipe.close();
}

TEST(PipelineContracts, ClosingWaitsForReservedWriterBeforeEndOfStream)
{
    takt::Pipe<int> pipe("pending", 1, 0);
    std::optional<takt::Pipe<int>::WriteToken> write(pipe.acquire_write());
    pipe.close();
    auto reader = std::async(std::launch::async,
                             [&pipe]
                             {
                                 auto read = pipe.acquire_read();
                                 return read.value();
                             });
    EXPECT_EQ(reader.wait_for(50ms), std::future_status::timeout);
    write->value() = 42;
    write->publish();
    write.reset();
    EXPECT_EQ(reader.get(), 42);
    EXPECT_THROW((void)pipe.acquire_read(), takt::PipeClosedAndEmptySignal);
}

TEST(PipelineContracts, MultipleReadTokensRetainTheirOwnConsecutiveSegments)
{
    takt::Pipe<int> pipe("retained-reads", 2, 0);
    for (int value : {1, 2})
    {
        auto write = pipe.acquire_write();
        write.value() = value;
        write.publish();
    }
    std::optional<takt::Pipe<int>::ReadToken> first(pipe.acquire_read());
    pipe.close();
    auto reader = std::async(std::launch::async,
                             [&pipe]
                             {
                                 auto second = pipe.acquire_read();
                                 return second.value();
                             });
    EXPECT_EQ(reader.wait_for(2s), std::future_status::ready);
    EXPECT_EQ(first->value(), 1);
    first.reset();
    EXPECT_EQ(reader.get(), 2);
}

TEST(PipelineContracts, ErrorClosureDrainsUnterminatedPrefixAndPendingWrites)
{
    takt::Pipe<int> pipe("error-prefix", 3, 0);
    {
        auto write = pipe.acquire_write(false);
        write.value() = 1;
        write.publish();
    }
    std::optional<takt::Pipe<int>::WriteToken> pending(pipe.acquire_write(false));
    pipe.close_with_error("upstream failed");
    {
        auto read = pipe.acquire_read();
        EXPECT_EQ(read.value(), 1);
    }
    auto reader = std::async(std::launch::async,
                             [&pipe]
                             {
                                 auto read = pipe.acquire_read();
                                 return read.value();
                             });
    EXPECT_EQ(reader.wait_for(50ms), std::future_status::timeout);
    pending->value() = 2;
    pending->publish();
    pending.reset();
    EXPECT_EQ(reader.get(), 2);
    EXPECT_TRUE(pipe.faulted());
    EXPECT_EQ(pipe.fault_message(), "upstream failed");
    EXPECT_THROW((void)pipe.acquire_read(), takt::PipeFaultedError);
    EXPECT_THROW((void)pipe.acquire_write(), takt::PipeFaultedError);
    EXPECT_THROW(pipe.close_with_error(""), std::invalid_argument);
}

TEST(PipelineContracts, AbandonedMiddleWriteDoesNotPublishLaterFramesAcrossGap)
{
    takt::Pipe<int> pipe("gap", 3, 0);
    std::optional<takt::Pipe<int>::WriteToken> first(pipe.acquire_write(false));
    std::optional<takt::Pipe<int>::WriteToken> failed(pipe.acquire_write(false));
    std::optional<takt::Pipe<int>::WriteToken> last(pipe.acquire_write(true));
    first->value() = 1;
    first->publish();
    last->value() = 3;
    last->publish();
    last.reset();
    failed.reset();
    first.reset();
    {
        auto read = pipe.acquire_read();
        EXPECT_EQ(read.value(), 1);
    }
    EXPECT_EQ(pipe.size(), 0U);
    EXPECT_THROW((void)pipe.acquire_read(), takt::PipeFaultedError);
}

TEST(PipelineContracts, TypedBridgeTransfersVectorStorageInsteadOfCopyingPixels)
{
    auto input =
        std::make_shared<FramePipe>("bridge-in", 1, Frame(16), takt::PipeDescriptor{},
                                    takt::ReadHistoryPolicy::Disabled);
    auto output =
        std::make_shared<FramePipe>("bridge-out", 1, Frame(16), takt::PipeDescriptor{},
                                    takt::ReadHistoryPolicy::Disabled);
    const float* original = nullptr;
    {
        auto write = input->acquire_write();
        original = write.value().data();
        write.value()[7] = 42.0F;
        write.publish();
    }
    auto source = takt::make_pipe_endpoint(input);
    auto target = takt::make_pipe_endpoint(output);
    source->transfer_to(*target);
    {
        auto read = output->acquire_read();
        EXPECT_EQ(read.value().data(), original);
        EXPECT_EQ(read.value()[7], 42.0F);
    }
    EXPECT_THROW(source->transfer_to(*source), std::invalid_argument);
    {
        auto write = input->acquire_write();
        write.publish();
    }
    auto mismatch =
        takt::make_pipe_endpoint(std::make_shared<takt::Pipe<double>>("other", 1));
    EXPECT_THROW(source->transfer_to(*mismatch), takt::PipeEndpointTypeMismatchError);
    source->close_with_error("bridge fault");
    EXPECT_THROW(source->transfer_to(*target), takt::PipeFaultedError);
    target->close();
}

TEST(PipelineContracts, NodeAdmissionIsBoundedWhileWorkerIsBusy)
{
    std::promise<void> release;
    BlockedNode node(release.get_future().share());
    auto ready = node.second_built.get_future();
    node.start();
    EXPECT_EQ(ready.wait_for(2s), std::future_status::ready);
    EXPECT_EQ(node.task_queue_capacity(), 1U);
    std::this_thread::sleep_for(50ms);
    EXPECT_EQ(node.built.load(), 2);
    release.set_value();
    node.join();
    EXPECT_EQ(node.built.load(), 4);
}

TEST(PipelineContracts, TokenOwnedFramesDrainAndWorkerFailureReachesRuntime)
{
    for (bool failure : {false, true})
    {
        FramePipe input("frames-in", 2, Frame(16), {},
                        takt::ReadHistoryPolicy::Disabled);
        FramePipe output("frames-out", 2, Frame(16), {},
                         takt::ReadHistoryPolicy::Disabled);
        auto node = std::make_shared<FrameNode>(input, output, failure);
        takt::PipelineRuntime runtime({node});
        runtime.start();
        auto producer = std::async(std::launch::async,
                                   [&input]
                                   {
                                       try
                                       {
                                           for (int i = 1; i <= 4; ++i)
                                           {
                                               auto write = input.acquire_write();
                                               write.value()[0] = static_cast<float>(i);
                                               write.publish();
                                           }
                                           input.close();
                                       }
                                       catch (const takt::PipeFaultedError&)
                                       {
                                       }
                                   });
        size_t completed = 0;
        bool faulted = false;
        while (true)
        {
            try
            {
                auto read = output.acquire_read();
                ++completed;
                EXPECT_EQ(read.value()[0], static_cast<float>(completed) * 2.0F);
            }
            catch (const takt::PipeClosedAndEmptySignal&)
            {
                break;
            }
            catch (const takt::PipeFaultedError&)
            {
                faulted = true;
                break;
            }
        }
        producer.get();
        if (failure)
        {
            try
            {
                runtime.join();
                ADD_FAILURE() << "expected injected failure";
            }
            catch (const std::runtime_error& error)
            {
                EXPECT_EQ(std::string(error.what()), "injected frame failure");
            }
            EXPECT_TRUE(node->failure());
        }
        else
        {
            EXPECT_NO_THROW(runtime.join());
            EXPECT_FALSE(node->failure());
        }
        EXPECT_EQ(completed, failure ? 2U : 4U);
        EXPECT_EQ(faulted, failure);
        EXPECT_TRUE(node->storage_preserved.load());
    }
}
