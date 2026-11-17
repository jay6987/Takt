// Verifies ordered direct/mixed boundary transfer and complete mapping pairs.
#include "subgraph_direct_tests_test_support.h"

namespace
{
// Publishes a known ordered frame sequence and closes the mapping input.
void write_mapping_frames(const std::shared_ptr<takt::Pipe<int>>& input)
{
    for (int frame = 0; frame < 16; ++frame)
    {
        auto write = input->acquire_shot_end_write(1);
        write.value() = frame;
        write.publish();
    }
    input->close();
}

// Collects mapped output values until the nested graph closes normally.
std::vector<int>
read_mapping_frames(const std::shared_ptr<takt::Pipe<int>>& output)
{
    std::vector<int> values;
    try
    {
        while (true)
        {
            auto read = output->acquire_read();
            values.push_back(read.value());
        }
    }
    catch (const takt::PipeClosedAndEmptySignal&)
    {
    }
    return values;
}

// Checks mapped worker counts, bounded completion, and ordered doubling.
void verify_mapping_transfer(
    const std::shared_ptr<takt::Pipe<int>>& input,
    const std::shared_ptr<takt::Pipe<int>>& output,
    const std::shared_ptr<takt::PipelineRuntime>& runtime,
    const std::shared_ptr<takt::SubgraphNode>& node)
{
    runtime->initialize();
    runtime->start();
    EXPECT_EQ(node->launched_bridge_worker_count(), node->bridge_count());
    auto writer = std::async(std::launch::async, write_mapping_frames, input);
    auto reader = std::async(std::launch::async, read_mapping_frames, output);
    const auto status = reader.wait_for(5s);
    if (status != std::future_status::ready)
        runtime->request_stop();
    EXPECT_EQ(status, std::future_status::ready);
    writer.get();
    const auto values = reader.get();
    ASSERT_EQ(values.size(), 16U);
    for (size_t frame = 0; frame < values.size(); ++frame)
        EXPECT_EQ(values[frame], static_cast<int>(frame) * 2);
    runtime->join();
}
} // namespace

// Verifies direct and mixed boundaries both preserve ordered frame delivery.
TEST(SubgraphDirect,
     AllDirectAndMixedMappingsTransferOrderedFrames)
{
    for (const size_t capacity : {1U, 2U})
        for (const bool bridge_input : {false, true})
            for (const bool bridge_output : {false, true})
            {
                auto input = pipe("input", capacity);
                auto output = pipe("output", capacity);
                auto inner_input =
                    bridge_input ? pipe("inner-input", capacity) : input;
                auto inner_output =
                    bridge_output ? pipe("inner-output", capacity) : output;
                auto inner = relay_graph(inner_input, inner_output);
                auto outer = wrap(inner, input, output);
                auto node = std::dynamic_pointer_cast<takt::SubgraphNode>(
                    outer.find_bound_node("group"));
                ASSERT_NE(node, nullptr);
                EXPECT_EQ(node->mapping_count(), 2U);
                EXPECT_EQ(node->bridge_count(),
                          static_cast<size_t>(bridge_input) +
                              static_cast<size_t>(bridge_output));
                const auto snapshot = outer.graph().subgraphs.at("group");
                EXPECT_EQ(snapshot.input_transports.at("in").mode,
                          bridge_input ? takt::MappingTransport::Bridge
                                       : takt::MappingTransport::Direct);
                EXPECT_EQ(snapshot.output_transports.at("out").mode,
                          bridge_output ? takt::MappingTransport::Bridge
                                        : takt::MappingTransport::Direct);
                auto runtime = outer.build_runtime();
                verify_mapping_transfer(input, output, runtime, node);
            }
}

// Verifies direct construction registers lifecycle pipes and complete pairs.
TEST(SubgraphDirect,
     DirectConstructionRegistersPipesAndRejectsMissingPairs)
{
    auto shared = pipe("shared");
    auto inner = std::make_shared<takt::PipelineRuntime>(
        std::vector<std::shared_ptr<takt::NodeBase>>{});
    const std::unordered_map<std::string, takt::NodePortRef> mappings{
        {"in", {"worker", "in"}}};
    EXPECT_THROW((takt::SubgraphNode("bad", inner, mappings)),
                 std::logic_error);
    EXPECT_THROW((takt::SubgraphNode(
                     "bad", inner, mappings, {},
                     {{nullptr, takt::make_pipe_endpoint(shared), "", ""}})),
                 std::invalid_argument);
    takt::SubgraphNode node(
        "direct", inner, mappings, {},
        {{takt::make_pipe_endpoint(shared), takt::make_pipe_endpoint(shared),
          "outer.in", "worker.in"}});
    EXPECT_THROW(node.start(), std::logic_error);
    inner->initialize();
    EXPECT_TRUE(shared->initialized());
    EXPECT_THROW(shared->initialize(), std::logic_error);
    node.start();
    EXPECT_EQ(node.bridge_count(), 0U);
    EXPECT_EQ(node.launched_bridge_worker_count(), 0U);
    node.request_stop();
    node.request_stop();
    node.join();
    EXPECT_TRUE(shared->closed());
}
