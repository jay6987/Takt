// Verifies pipeline execution contracts with realistic source and sink nodes.
#include "pipe_test_support.h"
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
    // Creates a payload whose subsequent copies are counted.
    CopyProbe() = default;
    // Counts construction copies to detect unnecessary frame duplication.
    CopyProbe(const CopyProbe&)
    {
        ++copies;
    }
    // Counts assignment copies to detect unnecessary payload duplication.
    CopyProbe& operator=(const CopyProbe&)
    {
        ++copies;
        return *this;
    }
};

class FrameNode final : public takt::Node
{
  public:
    // Binds vector frame pipes with optional deterministic worker failure.
    FrameNode(FramePipe& input,
              FramePipe& output,
              bool inject_failure)
        : Node("frame"),
          input_(input),
          output_(output),
          inject_failure_(inject_failure)
    {
    }
    // Stops and joins workers before referenced pipe members are destroyed.
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
        // Retains input/output tokens and the original input storage address.
        FrameTask(FramePipe::ReadToken input_token,
                  FramePipe::WriteToken output_token)
            : input(std::move(input_token)),
              output(std::move(output_token)),
              reserved_address(input.value().data())
        {
        }
        FramePipe::ReadToken input;
        FramePipe::WriteToken output;
        const float* reserved_address;
    };
    // Reserves token-owned frames until the input stream is drained.
    std::unique_ptr<Task> build_task(uint64_t) override
    {
        try
        {
            auto input = input_.acquire_read();
            auto output = input.is_shot_end()
                              ? output_.acquire_shot_end_write(1)
                              : output_.acquire_write();
            return std::make_unique<FrameTask>(std::move(input),
                                               std::move(output));
        }
        catch (const takt::PipeClosedAndEmptySignal&)
        {
            return nullptr;
        }
    }
    // Doubles vector elements and checks token-owned input storage stability.
    void process_task(size_t,
                      Task& task) override
    {
        auto& frame = static_cast<FrameTask&>(task);
        if (inject_failure_ && task.sequence_id == 2)
        {
            throw std::runtime_error("injected frame failure");
        }
        storage_preserved.store(storage_preserved.load() &&
                                frame.input.value().data() ==
                                    frame.reserved_address);
        for (size_t i = 0; i < frame.input.value().size(); ++i)
        {
            frame.output.value()[i] = frame.input.value()[i] * 2.0F;
        }
        frame.output.publish();
    }
    // Closes output after no further frames can be admitted.
    void on_ingress_manager_done() override
    {
        output_.close();
    }
    // Faults both pipe boundaries and preserves standard failure logging.
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
    // Blocks processing on a shared signal to expose bounded ingress behavior.
    explicit BlockedNode(std::shared_future<void> release)
        : Node("bounded"),
          release_(std::move(release))
    {
    }
    std::atomic<int> built{0};
    std::promise<void> first_built;

  private:
    // Produces four tasks and signals when the first task is built.
    std::unique_ptr<Task> build_task(uint64_t sequence) override
    {
        if (sequence == 4)
            return nullptr;
        auto task = std::make_unique<Task>();
        if (built.fetch_add(1) == 0)
            first_built.set_value();
        return task;
    }
    // Waits for the test to release the permanently blocked worker.
    void process_task(size_t,
                      Task&) override
    {
        release_.wait();
    }
    std::shared_future<void> release_;
};

class ReservedTokenNode final : public takt::Node
{
  public:
    // Hold all workers to expose whether ingress prefetches pipe tokens.
    ReservedTokenNode(takt::Pipe<int>& input,
                      takt::Pipe<int>& output,
                      size_t workers,
                      std::shared_future<void> release)
        : Node("reserved",
               workers),
          input_(input),
          output_(output),
          release_(std::move(release))
    {
    }
    // Keep referenced tokens and synchronization state alive through join.
    ~ReservedTokenNode() override
    {
        request_stop();
        join();
    }
    std::promise<void> saturated;
    std::atomic<size_t> built{0}, started{0}, destroyed{0};

  private:
    struct TokenTask final : Task
    {
        ReservedTokenNode& owner;
        takt::Pipe<int>::ReadToken input;
        takt::Pipe<int>::WriteToken output;
        // Count cleanup after processing rather than merely counting returns.
        explicit TokenTask(ReservedTokenNode& node) : owner(node) {}
        // Record retirement of each handed-off payload.
        ~TokenTask() override
        {
            ++owner.destroyed;
        }
    };
    // Acquire tokens only after a worker has been reserved for this payload.
    std::unique_ptr<Task> build_task(uint64_t sequence) override
    {
        if (sequence == worker_count() + 1)
            return nullptr;
        auto task = std::make_unique<TokenTask>(*this);
        task->input = input_.acquire_read();
        task->output = output_.acquire_shot_end_write(1);
        ++built;
        return task;
    }
    // Hold the workers until the test observes lazy acquisition.
    void process_task(size_t,
                      Task& base) override
    {
        auto& task = static_cast<TokenTask&>(base);
        if (++started == worker_count())
            saturated.set_value();
        release_.wait();
        task.output.value() = task.input.value() * 2;
        task.output.publish();
    }
    takt::Pipe<int>& input_;
    takt::Pipe<int>& output_;
    std::shared_future<void> release_;
};

class PreparingTokenNode final : public takt::Node
{
  public:
    // Hold ingress after acquisition to exercise cancellation of a reservation.
    PreparingTokenNode(takt::Pipe<int>& input,
                       takt::Pipe<int>& output,
                       std::shared_future<void> release)
        : Node("preparing",
               2),
          input_(input),
          output_(output),
          release_(std::move(release))
    {
    }
    // Preserve hook and task state until all framework threads have joined.
    ~PreparingTokenNode() override
    {
        request_stop();
        join();
    }
    std::promise<void> preparing;
    std::atomic<size_t> destroyed{0}, processed{0};

  private:
    struct TokenTask final : Task
    {
        PreparingTokenNode& owner;
        takt::Pipe<int>::ReadToken input;
        takt::Pipe<int>::WriteToken output;
        // Track a payload whose tokens have been acquired but not handed off.
        explicit TokenTask(PreparingTokenNode& node) : owner(node) {}
        // Count destruction when ingress cancels the pending handoff.
        ~TokenTask() override
        {
            ++owner.destroyed;
        }
    };
    // Simulate ingress preparation while keeping both tokens in the payload.
    std::unique_ptr<Task> build_task(uint64_t) override
    {
        auto task = std::make_unique<TokenTask>(*this);
        task->input = input_.acquire_read();
        task->output = output_.acquire_write();
        preparing.set_value();
        release_.wait();
        return task;
    }
    // Cancellation must prevent this payload from reaching a reserved worker.
    void process_task(size_t,
                      Task&) override
    {
        ++processed;
    }
    takt::Pipe<int>& input_;
    takt::Pipe<int>& output_;
    std::shared_future<void> release_;
};
} // namespace

// Verifies overlapping read windows reuse payload storage without copying it.
TEST(PipelineContracts,
     OverlapWindowsReuseRingSlotsWithoutCopyingPayloads)
{
    takt::Pipe<CopyProbe> pipe("no-history");
    configure_pipe(pipe, 3, CopyProbe{}, 2, 1, 3);
    pipe.initialize();
    {
        auto write = pipe.acquire_shot_end_write(3);
        write.publish();
    }
    CopyProbe::copies = 0;

    auto first = pipe.acquire_read();
    auto second = pipe.acquire_read();
    ASSERT_EQ(second.overlap_frames(), 1U);
    EXPECT_EQ(&first.value(1), &second.value(0));
    EXPECT_EQ(CopyProbe::copies, 0);
    pipe.close();
}

// Verifies same-name builders are independent and release bound nodes.
TEST(PipelineContracts,
     SameNameBuildersAreIndependentAndReleaseBoundNodes)
{
    std::weak_ptr<takt::NodeBase> weak;
    {
        auto first = takt::PipelineBuilder::create("scoped");
        first.add_node("first");
        auto bound = std::make_shared<BlockedNode>(std::shared_future<void>{});
        weak = bound;
        first.bind_node("first", bound);
        auto second = takt::PipelineBuilder::create("scoped");
        EXPECT_TRUE(second.nodes().empty());
    }
    EXPECT_TRUE(weak.expired());
    auto rebuilt = takt::PipelineBuilder::create("scoped");
    EXPECT_TRUE(rebuilt.nodes().empty());
    EXPECT_FALSE(rebuilt.has_bound_node("first"));
}

// Verifies closure waits for outstanding writes before signaling completion.
TEST(PipelineContracts,
     ClosingWaitsForReservedWriterBeforeEndOfStream)
{
    takt::Pipe<int> pipe("pending");
    configure_pipe(pipe, 1, 0);
    pipe.initialize();
    std::optional<takt::Pipe<int>::WriteToken> write(pipe.acquire_write());
    auto reader = std::async(std::launch::async,
                             [&pipe]
                             {
                                 auto read = pipe.acquire_read();
                                 return read.value();
                             });
    EXPECT_EQ(reader.wait_for(50ms), std::future_status::timeout);
    EXPECT_THROW(pipe.close(), takt::PipeUnterminatedShotError);
    EXPECT_TRUE(pipe.closed());
    EXPECT_EQ(reader.wait_for(50ms), std::future_status::timeout);
    write->value() = 42;
    write->publish();
    write.reset();
    EXPECT_EQ(reader.get(), 42);
    EXPECT_THROW((void)pipe.acquire_read(), takt::PipeClosedAndEmptySignal);
}

// Verifies an unterminated closed shot drains without inventing a final marker.
TEST(PipelineContracts,
     ClosedUnterminatedShotDrainsPartialWindowWithoutShotEnd)
{
    takt::Pipe<int> pipe("partial-unended-tail");
    configure_pipe(pipe, 3, 0, 3, 0, 1);
    pipe.initialize();
    for (int value : {4, 5})
    {
        auto write = pipe.acquire_write();
        write.value() = value;
        write.publish();
    }

    EXPECT_THROW(pipe.close(), takt::PipeUnterminatedShotError);
    {
        auto tail = pipe.acquire_read();
        EXPECT_EQ(tail.frame_count(), 2U);
        EXPECT_FALSE(tail.is_shot_end());
        EXPECT_EQ(tail.value(0), 4);
        EXPECT_EQ(tail.value(1), 5);
    }
    EXPECT_THROW((void)pipe.acquire_read(), takt::PipeClosedAndEmptySignal);
}

// Verifies concurrent readable windows keep distinct consecutive storage.
TEST(PipelineContracts,
     MultipleReadTokensRetainTheirOwnConsecutiveWindows)
{
    takt::Pipe<int> pipe("retained-reads");
    configure_pipe(pipe, 2, 0);
    pipe.initialize();
    for (int value : {1, 2})
    {
        auto write = pipe.acquire_shot_end_write(1);
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

// Verifies failure closure preserves readable committed and pending prefixes.
TEST(PipelineContracts,
     ErrorClosureDrainsUnterminatedPrefixAndPendingWrites)
{
    takt::Pipe<int> pipe("error-prefix");
    configure_pipe(pipe, 3, 0);
    pipe.initialize();
    {
        auto write = pipe.acquire_write();
        write.value() = 1;
        write.publish();
    }
    std::optional<takt::Pipe<int>::WriteToken> pending(pipe.acquire_write());
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

// Verifies a cancelled middle reservation blocks publication across the gap.
TEST(PipelineContracts,
     AbandonedMiddleWriteDoesNotPublishLaterFramesAcrossGap)
{
    takt::Pipe<int> pipe("gap");
    configure_pipe(pipe, 3, 0);
    pipe.initialize();
    std::optional<takt::Pipe<int>::WriteToken> first(pipe.acquire_write());
    std::optional<takt::Pipe<int>::WriteToken> failed(pipe.acquire_write());
    std::optional<takt::Pipe<int>::WriteToken> last(
        pipe.acquire_shot_end_write(1));
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
    EXPECT_EQ(pipe.queued_frame_count(), 0U);
    EXPECT_THROW((void)pipe.acquire_read(), takt::PipeFaultedError);
}

// Verifies typed bridges swap vector storage rather than copying payloads.
TEST(PipelineContracts,
     TypedBridgeTransfersVectorStorageInsteadOfCopyingPixels)
{
    auto input = make_configured_pipe("bridge-in", 1, Frame(16));
    auto output = make_configured_pipe("bridge-out", 1, Frame(16));
    input->initialize();
    output->initialize();
    const float* original = nullptr;
    {
        auto write = input->acquire_shot_end_write(1);
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
        auto write = input->acquire_shot_end_write(1);
        write.publish();
    }
    auto mismatch =
        takt::make_pipe_endpoint(std::make_shared<takt::Pipe<double>>("other"));
    EXPECT_THROW(source->transfer_to(*mismatch),
                 takt::PipeEndpointTypeMismatchError);
    source->close_with_error("bridge fault");
    EXPECT_THROW(source->transfer_to(*target), takt::PipeFaultedError);
    target->close();
}

// Verifies ingress does not build ahead while its only worker is busy.
TEST(PipelineContracts,
     NodeAdmissionIsLazyWhileWorkerIsBusy)
{
    std::promise<void> release;
    BlockedNode node(release.get_future().share());
    auto ready = node.first_built.get_future();
    node.start();
    EXPECT_EQ(ready.wait_for(2s), std::future_status::ready);
    std::this_thread::sleep_for(50ms);
    EXPECT_EQ(node.built.load(), 1);
    release.set_value();
    node.join();
    EXPECT_EQ(node.built.load(), 4);
}

// Verify neither read nor write tokens are prefetched without an idle worker.
TEST(PipelineContracts,
     BusyWorkersPreventTokenAcquisitionAndStopWakesAdmission)
{
    for (size_t workers : {1U, 3U})
        for (bool stop : {false, true})
        {
            takt::Pipe<int> input("lazy-input");
            takt::Pipe<int> output("lazy-output");
            configure_pipe(input, workers + 1, 0);
            configure_pipe(output, workers + 1, 0);
            input.initialize();
            output.initialize();
            for (size_t i = 0; i <= workers; ++i)
            {
                auto write = input.acquire_shot_end_write(1);
                write.value() = static_cast<int>(i);
                write.publish();
            }
            std::promise<void> release;
            ReservedTokenNode node(input, output, workers,
                                   release.get_future().share());
            auto ready = node.saturated.get_future();
            node.start();
            EXPECT_EQ(ready.wait_for(2s), std::future_status::ready);
            std::this_thread::sleep_for(50ms);
            EXPECT_EQ(node.built.load(), workers);
            EXPECT_EQ(input.queued_frame_count(), 1U);
            EXPECT_EQ(output.queued_frame_count(), 0U);
            if (stop)
                node.request_stop();
            release.set_value();
            node.join();
            EXPECT_EQ(node.failure(), nullptr);
            EXPECT_EQ(node.built.load(), workers + (stop ? 0 : 1));
            EXPECT_EQ(node.destroyed.load(), node.built.load());
            for (size_t i = 0; i < node.built.load(); ++i)
            {
                auto read = output.acquire_read();
                EXPECT_EQ(read.value(), static_cast<int>(i) * 2);
            }
        }
}

// Stop during preparation cancels the reserved worker and uncommitted tokens.
TEST(PipelineContracts,
     StopDuringTaskPreparationCancelsHandoffAndReleasesTokens)
{
    takt::Pipe<int> input("preparing-input");
    takt::Pipe<int> output("preparing-output");
    configure_pipe(input, 1, 0);
    configure_pipe(output, 1, 0);
    input.initialize();
    output.initialize();
    {
        auto write = input.acquire_shot_end_write(1);
        write.publish();
    }
    std::promise<void> release;
    PreparingTokenNode node(input, output, release.get_future().share());
    auto ready = node.preparing.get_future();
    node.start();
    EXPECT_EQ(ready.wait_for(2s), std::future_status::ready);
    node.request_stop();
    release.set_value();
    node.join();
    EXPECT_EQ(node.failure(), nullptr);
    EXPECT_EQ(node.processed.load(), 0U);
    EXPECT_EQ(node.destroyed.load(), 1U);
    EXPECT_TRUE(output.faulted());
    EXPECT_NO_THROW((void)input.acquire_write());
}

// Verifies token-owned frames drain and processing failures reach runtime join.
TEST(PipelineContracts,
     TokenOwnedFramesDrainAndWorkerFailureReachesRuntime)
{
    for (bool failure : {false, true})
    {
        FramePipe input("frames-in");
        FramePipe output("frames-out");
        configure_pipe(input, 2, Frame(16));
        configure_pipe(output, 2, Frame(16));
        input.initialize();
        output.initialize();
        auto node = std::make_shared<FrameNode>(input, output, failure);
        takt::PipelineRuntime runtime({node});
        runtime.initialize();
        runtime.start();
        auto producer =
            std::async(std::launch::async,
                       [&input]
                       {
                           try
                           {
                               for (int i = 1; i <= 4; ++i)
                               {
                                   auto write = input.acquire_shot_end_write(1);
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
                EXPECT_EQ(read.value()[0],
                          static_cast<float>(completed) * 2.0F);
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
