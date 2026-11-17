// Verifies draining, publication gaps, zero-copy bridges, and admission.
#include "pipeline_contract_tests_test_support.h"

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
