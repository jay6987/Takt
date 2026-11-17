// Verifies nested bridge data flow and type-erased endpoint mismatch errors.
#include "pipeline_builder_tests_test_support.h"

namespace
{
// Builds a nested doubling runtime with independent boundary bridge pipes.
std::shared_ptr<takt::PipelineRuntime>
make_bridge_runtime(const std::shared_ptr<takt::Pipe<int>>& outer_in,
                    const std::shared_ptr<takt::Pipe<int>>& outer_out,
                    const std::shared_ptr<takt::Pipe<int>>& inner_in,
                    const std::shared_ptr<takt::Pipe<int>>& inner_out)
{
    auto inner = takt::PipelineBuilder::create("graph-inner-bridge");
    inner.add_node("worker");
    inner.add_input_port(
        "worker", "in",
        {{"pipe_endpoint", takt::make_pipe_endpoint<int>(inner_in)}});
    inner.add_output_port(
        "worker", "out",
        {{"pipe_endpoint", takt::make_pipe_endpoint<int>(inner_out)}});
    inner.bind_node("worker",
                    std::make_shared<DoubleNode>(inner_in, inner_out));
    auto outer = takt::PipelineBuilder::create("graph-outer-bridge");
    outer.add_node("sg");
    outer.add_input_port(
        "sg", "raw",
        {{"pipe_endpoint", takt::make_pipe_endpoint<int>(outer_in)}});
    outer.add_output_port(
        "sg", "result",
        {{"pipe_endpoint", takt::make_pipe_endpoint<int>(outer_out)}});
    outer.bind_subgraph("sg", inner, {{"raw", {"worker", "in"}}},
                        {{"result", {"worker", "out"}}});
    return outer.build_runtime();
}

// Publishes four ordered input frames and closes their source pipe.
void publish_bridge_inputs(takt::Pipe<int>& input)
{
    for (int value = 1; value <= 4; ++value)
    {
        auto write = input.acquire_shot_end_write(1);
        write.value() = value;
        write.publish();
    }
    input.close();
}

// Collects bridge output values until normal end-of-stream closure.
std::vector<int> drain_bridge_outputs(takt::Pipe<int>& output)
{
    std::vector<int> result;
    while (true)
    {
        try
        {
            auto read = output.acquire_read();
            result.push_back(read.value());
        }
        catch (const takt::PipeClosedAndEmptySignal&)
        {
            break;
        }
    }
    return result;
}
} // namespace

// Verifies bridge workers transfer frames through nested processing.
TEST(PipelineBuilderTests,
     SubgraphMappingBridgesRuntimeDataFlow)
{
    auto outer_in = make_configured_pipe("outer-in", 16, 0);
    auto outer_out = make_configured_pipe("outer-out", 16, 0);
    auto inner_in = make_configured_pipe("inner-in", 16, 0);
    auto inner_out = make_configured_pipe("inner-out", 16, 0);

    auto runtime =
        make_bridge_runtime(outer_in, outer_out, inner_in, inner_out);
    runtime->initialize();
    runtime->start();
    publish_bridge_inputs(*outer_in);
    runtime->join();
    const auto result = drain_bridge_outputs(*outer_out);
    ASSERT_EQ(result.size(), 4U);
    EXPECT_EQ(result[0], 2);
    EXPECT_EQ(result[1], 4);
    EXPECT_EQ(result[2], 6);
    EXPECT_EQ(result[3], 8);
}

// Verifies type-erased writes reject incompatible payload types.
TEST(PipelineBuilderTests,
     PipeEndpointWriteReportsTypeMismatch)
{
    auto pipe = make_configured_pipe("type-mismatch", 4, 0);
    pipe->initialize();
    auto endpoint = takt::make_pipe_endpoint<int>(pipe);

    takt::AnyPipePacket packet;
    packet.value = std::string("not-an-int");

    try
    {
        endpoint->write(packet);
        FAIL() << "expected PipeEndpointTypeMismatchError";
    }
    catch (const takt::PipeEndpointTypeMismatchError& e)
    {
        const std::string message = e.what();
        EXPECT_NE(message.find("type-mismatch"), std::string::npos);
        EXPECT_NE(message.find("expected='"), std::string::npos);
        EXPECT_NE(message.find("actual='"), std::string::npos);
    }
}
