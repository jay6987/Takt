// Verifies direct subgraph startup cleanup and blocked endpoint cancellation.
#include "subgraph_direct_tests_test_support.h"

namespace
{
// Checks partial startup closes both direct boundaries and permits joining.
void verify_partial_startup_cleanup()
{
    auto input = pipe("input");
    auto output = pipe("output");
    auto inner = relay_graph(input, output);
    inner.add_node("fail").bind_node("fail", std::make_shared<FailingNode>());
    auto outer = wrap(inner, input, output);
    auto root = outer.build_runtime();
    root->initialize();
    EXPECT_THROW(root->start(), std::runtime_error);
    EXPECT_TRUE(input->closed());
    EXPECT_TRUE(output->closed());
    EXPECT_NO_THROW(root->join());
}

// Blocks a producer or consumer until runtime cancellation closes its pipe.
void block_endpoint_operation(bool write_frames,
                              const std::shared_ptr<takt::Pipe<int>>& input,
                              const std::shared_ptr<takt::Pipe<int>>& output,
                              std::promise<void>& entered)
{
    entered.set_value();
    try
    {
        if (write_frames)
        {
            for (int frame = 0; frame < 64; ++frame)
            {
                auto write = input->acquire_shot_end_write(1);
                write.value() = frame;
                write.publish();
            }
        }
        else
        {
            (void)output->acquire_read();
        }
    }
    catch (const takt::PipeClosedAndEmptySignal&)
    {
    }
}

} // namespace

// Verifies partial startup cancellation closes shared direct boundary pipes.
TEST(SubgraphDirect,
     PartialStartupFailureClosesDirectBoundaries)
{
    verify_partial_startup_cleanup();
}

// Verifies stopping direct nesting releases both blocked endpoint operations.
TEST(SubgraphDirect,
     StopReleasesBlockedProducerAndConsumer)
{
    for (const bool write_frames : {false, true})
    {
        auto input = pipe("input");
        auto output = pipe("output");
        auto graph = wrap(relay_graph(input, output), input, output);
        auto root = graph.build_runtime();
        root->initialize();
        root->start();
        std::promise<void> entered;
        auto ready = entered.get_future();
        auto participant =
            std::async(std::launch::async, block_endpoint_operation,
                       write_frames, input, output, std::ref(entered));
        const auto status = ready.wait_for(5s);
        EXPECT_EQ(status, std::future_status::ready);
        EXPECT_EQ(participant.wait_for(20ms), std::future_status::timeout);
        root->request_stop();
        EXPECT_EQ(participant.wait_for(5s), std::future_status::ready);
        participant.get();
        EXPECT_NO_THROW(root->join());
    }
}
