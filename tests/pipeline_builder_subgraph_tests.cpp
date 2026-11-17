// Verifies subgraph construction, stored port mappings, and mapping validation.
#include "pipeline_builder_tests_test_support.h"

// Verifies nested topology becomes an executable composite runtime node.
TEST(PipelineBuilderTests,
     BindSubgraphBuildsCompositeNode)
{
    auto inner = takt::PipelineBuilder::create("graph-inner");
    inner.add_node("inner-a");
    inner.add_node("inner-b");

    std::atomic<int> inner_run_count{0};
    inner.bind_node("inner-a", std::make_shared<ProbeNode>(inner_run_count));
    inner.bind_node("inner-b", std::make_shared<ProbeNode>(inner_run_count));

    auto outer = takt::PipelineBuilder::create("graph-outer");
    outer.add_node("subgraph-entry");
    outer.bind_subgraph("subgraph-entry", inner);

    auto runtime = outer.build_runtime();
    runtime->initialize();
    runtime->start();
    runtime->join();

    EXPECT_EQ(inner_run_count.load(std::memory_order_relaxed), 2);
}

// Verifies explicit boundary mappings survive subgraph binding.
TEST(PipelineBuilderTests,
     BindSubgraphWithPortMappingsStoresMappings)
{
    auto outer_in = make_configured_pipe("mapped-outer-in", 8, 0);
    auto outer_out = make_configured_pipe("mapped-outer-out", 8, 0);
    auto inner_in = make_configured_pipe("mapped-inner-in", 8, 0);
    auto inner_out = make_configured_pipe("mapped-inner-out", 8, 0);

    auto inner = takt::PipelineBuilder::create("graph-inner-mapped");
    inner.add_node("entry");
    inner.add_node("exit");
    inner.add_input_port(
        "entry", "in",
        {{"pipe_endpoint", takt::make_pipe_endpoint<int>(inner_in)}});
    inner.add_output_port(
        "exit", "out",
        {{"pipe_endpoint", takt::make_pipe_endpoint<int>(inner_out)}});

    std::atomic<int> inner_run_count{0};
    inner.bind_node("entry", std::make_shared<ProbeNode>(inner_run_count));
    inner.bind_node("exit", std::make_shared<ProbeNode>(inner_run_count));

    auto outer = takt::PipelineBuilder::create("graph-outer-mapped");
    outer.add_node("sg");
    outer.add_input_port(
        "sg", "raw",
        {{"pipe_endpoint", takt::make_pipe_endpoint<int>(outer_in)}});
    outer.add_output_port(
        "sg", "result",
        {{"pipe_endpoint", takt::make_pipe_endpoint<int>(outer_out)}});

    outer.bind_subgraph("sg", inner, {{"raw", {"entry", "in"}}},
                        {{"result", {"exit", "out"}}});

    auto bound = outer.find_bound_node("sg");
    auto subgraph_node = std::dynamic_pointer_cast<takt::SubgraphNode>(bound);
    ASSERT_NE(subgraph_node, nullptr);
    ASSERT_EQ(subgraph_node->input_port_mappings().size(), 1U);
    ASSERT_EQ(subgraph_node->output_port_mappings().size(), 1U);
    EXPECT_EQ(subgraph_node->input_port_mappings().at("raw").node_name,
              "entry");
    EXPECT_EQ(subgraph_node->input_port_mappings().at("raw").port_name, "in");
    EXPECT_EQ(subgraph_node->output_port_mappings().at("result").node_name,
              "exit");
    EXPECT_EQ(subgraph_node->output_port_mappings().at("result").port_name,
              "out");
}

// Verifies incomplete boundary mappings are rejected before execution.
TEST(PipelineBuilderTests,
     BindSubgraphRejectsMissingRequiredMapping)
{
    auto inner = takt::PipelineBuilder::create("graph-inner-mapped-fail");
    inner.add_node("entry");
    inner.add_input_port("entry", "in");
    std::atomic<int> inner_run_count{0};
    inner.bind_node("entry", std::make_shared<ProbeNode>(inner_run_count));

    auto outer = takt::PipelineBuilder::create("graph-outer-mapped-fail");
    outer.add_node("sg");
    outer.add_input_port("sg", "raw");

    EXPECT_THROW(outer.bind_subgraph("sg", inner, {}, {}), std::logic_error);
}
