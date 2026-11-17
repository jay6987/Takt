// Verifies graph-model flexibility and runtime node metadata injection.
#include "pipeline_builder_tests_test_support.h"

// Verifies descriptive topology can represent cyclic connections.
TEST(PipelineBuilderTests,
     AllowsCyclesInGraphModel)
{
    auto builder = takt::PipelineBuilder::create("graph-cycle");
    builder.add_node("a");
    builder.add_node("b");
    builder.add_output_port("a", "out");
    builder.add_input_port("b", "in");
    builder.add_output_port("b", "out");
    builder.add_input_port("a", "in");

    builder.connect("a", "out", "b", "in");
    builder.connect("b", "out", "a", "in");

    EXPECT_EQ(builder.edges().size(), 2U);
    EXPECT_NO_THROW(builder.validate());
}

// Verifies repeated declarations of identical endpoint connections are
// accepted.
TEST(PipelineBuilderTests,
     AllowsDuplicateSameEndpointEdges)
{
    auto builder = takt::PipelineBuilder::create("graph-duplicate-edges");
    builder.add_node("source");
    builder.add_node("sink");
    builder.add_output_port("source", "out");
    builder.add_input_port("sink", "in");

    builder.connect("source", "out", "sink", "in", {}, "e0");
    builder.connect("source", "out", "sink", "in", {}, "e1");

    EXPECT_EQ(builder.edges().size(), 2U);
    EXPECT_NO_THROW(builder.validate());
}

// Verifies runtime nodes receive connected-port diagnostic context.
TEST(PipelineBuilderTests,
     BuildRuntimeInjectsPortMetadataToBoundNodes)
{
    auto builder = takt::PipelineBuilder::create("graph-runtime-meta");
    builder.add_node("source");
    builder.add_node("transform");
    builder.add_output_port("source", "frames");
    builder.add_output_port("source", "masks");
    builder.add_input_port("transform", "frames");
    builder.add_input_port("transform", "masks");
    builder.connect("source", "frames", "transform", "frames");
    builder.connect("source", "masks", "transform", "masks");

    std::atomic<int> c0{0};
    std::atomic<int> c1{0};
    auto source = std::make_shared<ProbeNode>(c0);
    auto transform = std::make_shared<ProbeNode>(c1);

    builder.bind_node("source", source);
    builder.bind_node("transform", transform);

    auto runtime = builder.build_runtime();
    ASSERT_NE(runtime, nullptr);

    ASSERT_EQ(source->output_port_names().size(), 2U);
    EXPECT_EQ(source->output_port_names()[0], "frames");
    EXPECT_EQ(source->output_port_names()[1], "masks");
    ASSERT_TRUE(source->input_port_names().empty());

    ASSERT_EQ(transform->input_port_names().size(), 2U);
    EXPECT_EQ(transform->input_port_names()[0], "frames");
    EXPECT_EQ(transform->input_port_names()[1], "masks");
    ASSERT_TRUE(transform->output_port_names().empty());
}

// Verifies unconnected declared ports still appear in node diagnostics.
TEST(PipelineBuilderTests,
     BuildRuntimeInjectsDeclaredPortsWithoutChannels)
{
    auto builder = takt::PipelineBuilder::create("graph-runtime-declared-only");
    builder.add_node("standalone");
    builder.add_input_port("standalone", "in");
    builder.add_output_port("standalone", "out");

    std::atomic<int> count{0};
    auto node = std::make_shared<ProbeNode>(count);
    builder.bind_node("standalone", node);

    auto runtime = builder.build_runtime();
    ASSERT_NE(runtime, nullptr);
    ASSERT_EQ(node->input_port_names().size(), 1U);
    ASSERT_EQ(node->output_port_names().size(), 1U);
    EXPECT_EQ(node->input_port_names()[0], "in");
    EXPECT_EQ(node->output_port_names()[0], "out");
}

// Verifies bound runtime nodes actually execute and join.
TEST(PipelineBuilderTests,
     RuntimeStartsAndJoinsBoundNodes)
{
    auto builder = takt::PipelineBuilder::create("graph-runtime-run");
    builder.add_node("a");
    builder.add_node("b");

    std::atomic<int> run_count{0};
    builder.bind_node("a", std::make_shared<ProbeNode>(run_count));
    builder.bind_node("b", std::make_shared<ProbeNode>(run_count));

    auto runtime = builder.build_runtime();
    runtime->initialize();
    runtime->start();
    runtime->join();

    EXPECT_EQ(run_count.load(std::memory_order_relaxed), 2);
}
