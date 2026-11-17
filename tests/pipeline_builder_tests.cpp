// Verifies builder node, port, edge, and topology validation.
#include "pipeline_builder_tests_test_support.h"

// Verifies malformed declarations and connections fail without mutating state.
// Verifies malformed declarations and connections fail without mutating state.
TEST(PipelineBuilderTests,
     InvalidDeclarationsAndConnectionsAreRejected)
{
    auto graph = takt::PipelineBuilder::create("invalid-inputs");
    EXPECT_THROW(graph.add_node(""), std::invalid_argument);
    EXPECT_THROW(graph.add_input_port("", "in"), std::invalid_argument);
    EXPECT_THROW(graph.add_input_port("source", ""), std::invalid_argument);
    EXPECT_THROW(graph.add_output_port("", "out"), std::invalid_argument);
    EXPECT_THROW(graph.add_output_port("source", ""), std::invalid_argument);
    EXPECT_TRUE(graph.nodes().empty());
    graph.add_node("source").add_output_port("source", "out");
    graph.add_node("sink").add_input_port("sink", "in");
    EXPECT_THROW(graph.connect("", "out", "sink", "in"), std::invalid_argument);
    EXPECT_THROW(graph.connect("source", "", "sink", "in"),
                 std::invalid_argument);
    EXPECT_THROW(graph.connect("source", "out", "", "in"),
                 std::invalid_argument);
    EXPECT_THROW(graph.connect("source", "out", "sink", ""),
                 std::invalid_argument);
    EXPECT_THROW(graph.connect("missing", "out", "sink", "in"),
                 std::logic_error);
    EXPECT_THROW(graph.connect("source", "out", "missing", "in"),
                 std::logic_error);
    EXPECT_THROW(graph.connect("source", "missing", "sink", "in"),
                 std::logic_error);
    EXPECT_THROW(graph.connect("source", "out", "sink", "missing"),
                 std::logic_error);
    EXPECT_TRUE(graph.edges().empty());
    std::atomic<int> runs{0};
    auto probe = std::make_shared<ProbeNode>(runs);
    EXPECT_THROW(graph.bind_node("", probe), std::invalid_argument);
    EXPECT_THROW(graph.bind_node("source", nullptr), std::invalid_argument);
    EXPECT_THROW(graph.bind_node("missing", probe), std::logic_error);
    EXPECT_THROW((void)graph.build_runtime(), std::logic_error);
    EXPECT_FALSE(graph.has_bound_node("source"));
    EXPECT_FALSE(graph.has_node("missing"));
}

// Verifies metadata merging, missing lookups and shared builder-handle state.
TEST(PipelineBuilderTests,
     MetadataAndLookupContracts)
{
    auto graph = takt::PipelineBuilder::create("metadata");
    graph.add_node("source", {{"first", 1}});
    graph.add_node("source", {{"second", 2}});
    graph.add_output_port("source", "out", {{"first", 1}});
    graph.add_output_port("source", "out", {{"second", 2}});
    graph.add_input_port("sink", "in");
    graph.connect("source", "out", "sink", "in", {}, "frames");
    const auto* source = graph.find_node("source");
    ASSERT_NE(source, nullptr);
    EXPECT_EQ(source->descriptor.size(), 2U);
    ASSERT_EQ(source->output_ports.size(), 1U);
    EXPECT_EQ(source->output_ports[0].descriptor.size(), 2U);
    EXPECT_EQ(graph.find_node("absent"), nullptr);
    EXPECT_EQ(graph.find_edge("absent"), nullptr);
    ASSERT_NE(graph.find_edge("frames"), nullptr);
    EXPECT_EQ(graph.find_edge("frames")->to_node, "sink");
    EXPECT_EQ(graph.find_bound_node("absent"), nullptr);
    std::atomic<int> runs{0};
    auto probe = std::make_shared<ProbeNode>(runs);
    graph.bind_node("source", probe);
    EXPECT_EQ(graph.find_bound_node("source"), probe);
    auto copied = graph;
    copied.add_node("shared");
    EXPECT_TRUE(graph.has_node("shared"));
    auto assigned = takt::PipelineBuilder::create("replaced");
    assigned = graph;
    auto moved = std::move(copied);
    assigned = std::move(moved);
    EXPECT_EQ(assigned.name(), "metadata");
    EXPECT_EQ(assigned.nodes().size(), 3U);
}

// Verifies nodes can declare multiple independent input and output ports.
TEST(PipelineBuilderTests,
     SupportsMultipleInputsAndOutputs)
{
    auto builder = takt::PipelineBuilder::create("graph-multi");
    builder.add_node("source");
    builder.add_node("transform");
    builder.add_node("sink");

    builder.add_output_port("source", "out0");
    builder.add_output_port("source", "out1");
    builder.add_input_port("transform", "in0");
    builder.add_input_port("transform", "in1");
    builder.add_output_port("transform", "out");
    builder.add_input_port("sink", "left");
    builder.add_input_port("sink", "right");

    builder.connect("source", "out0", "transform", "in0");
    builder.connect("source", "out1", "transform", "in1");
    builder.connect("transform", "out", "sink", "left");
    builder.connect("transform", "out", "sink", "right");

    EXPECT_EQ(builder.nodes().size(), 3U);
    EXPECT_EQ(builder.edges().size(), 4U);
    EXPECT_NO_THROW(builder.validate());
}
