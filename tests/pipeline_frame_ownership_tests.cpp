// Verifies token-owned vector frames drain and worker failures reach runtimes.
#include "pipeline_contract_tests_test_support.h"

namespace
{
// Produces four vector frames while allowing injected pipeline faults.
std::future<void> produce_contract_frames(FramePipe& input)
{
    return std::async(std::launch::async,
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
}

// Checks transformed vector values and records the terminal fault state.
void drain_contract_output(FramePipe& output,
                           size_t& completed,
                           bool& faulted)
{
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
}

// Checks runtime joining preserves the expected original worker failure.
void verify_contract_runtime_result(takt::PipelineRuntime& runtime,
                                    const std::shared_ptr<FrameNode>& node,
                                    bool failure)
{
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
}
} // namespace

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
        auto producer = produce_contract_frames(input);
        size_t completed = 0;
        bool faulted = false;
        drain_contract_output(output, completed, faulted);
        producer.get();
        verify_contract_runtime_result(runtime, node, failure);
        EXPECT_EQ(completed, failure ? 2U : 4U);
        EXPECT_EQ(faulted, failure);
        EXPECT_TRUE(node->storage_preserved.load());
    }
}
