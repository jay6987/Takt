// Verifies direct boundary closure, nested initialization, and shared storage.
#include "subgraph_direct_tests_test_support.h"

namespace
{
// Checks direct output preserves its upstream fault or normal closure.
void verify_output_terminal_state(
    const std::shared_ptr<takt::Pipe<int>>& output,
    bool fault)
{
    if (fault)
    {
        EXPECT_EQ(output->acquire_read().value(), 42);
        EXPECT_THROW(output->acquire_read(), takt::PipeFaultedError);
    }
    else
    {
        EXPECT_THROW(output->acquire_read(), takt::PipeClosedAndEmptySignal);
    }
}
} // namespace

// Verifies direct boundaries propagate empty completion and upstream faults.
TEST(SubgraphDirect,
     EmptyInputAndFaultedPrefixPropagateWithoutBridge)
{
    for (const bool fault : {false, true})
    {
        auto input = pipe("input");
        auto output = pipe("output");
        auto graph = wrap(relay_graph(input, output), input, output);
        auto runtime = graph.build_runtime();
        runtime->initialize();
        if (fault)
        {
            auto write = input->acquire_write();
            write.value() = 21;
            write.publish();
        }
        if (fault)
            input->close_with_error("source fault");
        else
            input->close();
        runtime->start();
        auto result =
            std::async(std::launch::async,
                       [&] { verify_output_terminal_state(output, fault); });
        const auto status = result.wait_for(5s);
        if (status != std::future_status::ready)
            runtime->request_stop();
        EXPECT_EQ(status, std::future_status::ready);
        result.get();
        if (fault)
            EXPECT_THROW(runtime->join(), takt::PipeFaultedError);
        else
            EXPECT_NO_THROW(runtime->join());
    }
}

// Verifies nested direct pipes initialize and are closed after startup failure.
TEST(SubgraphDirect,
     NestedDirectInitializationAndStartupCleanup)
{
    for (const bool fail_start : {false, true})
    {
        auto input = pipe("input");
        auto output = pipe("output");
        auto middle = wrap(relay_graph(input, output), input, output);
        auto outer = takt::PipelineBuilder::create("outer");
        outer.add_node("nested")
            .add_input_port(
                "nested", "in",
                {{"pipe_endpoint", takt::make_pipe_endpoint(input)}})
            .add_output_port(
                "nested", "out",
                {{"pipe_endpoint", takt::make_pipe_endpoint(output)}})
            .bind_subgraph("nested", middle, {{"in", {"group", "in"}}},
                           {{"out", {"group", "out"}}});
        auto runtime = outer.build_runtime();
        if (!fail_start)
            runtime->initialize();
        else
            EXPECT_THROW(runtime->start(), std::logic_error);
        if (fail_start)
        {
            runtime->request_stop();
            EXPECT_TRUE(input->closed());
            EXPECT_THROW(runtime->initialize(), std::logic_error);
        }

        else
        {
            runtime->start();
            runtime->request_stop();
            EXPECT_NO_THROW(runtime->join());
            EXPECT_TRUE(input->closed());
            EXPECT_TRUE(output->closed());
        }
    }
}

// Verifies shared direct pipes allocate storage once across nested runtimes.
TEST(SubgraphDirect,
     SharedTypedPipesInitializeOnceAcrossNestedRuntimeTree)
{
    auto input = std::make_shared<CountingIntPipe>("same");
    auto output = std::make_shared<CountingIntPipe>("same");
    auto graph = wrap(relay_graph(input, output), input, output);
    auto root = graph.build_runtime();
    root->initialize();
    EXPECT_EQ(input->initializations, 1U);
    EXPECT_EQ(output->initializations, 1U);
    EXPECT_EQ(input->producer_config().name, "producer");
    EXPECT_EQ(input->consumer_config().name, "consumer");
    root->request_stop();
}
