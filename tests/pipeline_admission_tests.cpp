// Verifies bounded admission and cancellation during task preparation.
#include "pipeline_contract_tests_test_support.h"

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
