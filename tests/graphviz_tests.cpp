// Verifies DOT graph rendering, escaping, and topology validation.
#include "graphviz_tests_test_support.h"

// Verifies stable DOT escaping without serializing arbitrary descriptors.
// Verifies stable DOT escaping without serializing arbitrary descriptors.
TEST(Graphviz,
     ExportsStableEscapedTopologyWithoutDescriptors)
{
    auto graph = chain();
    graph.name = "quoted \"graph\"\\line\nnext\tcolumn\r";
    graph.nodes[0].descriptor["secret"] = std::string("not-exported");
    const auto dot = takt::to_dot(graph);
    EXPECT_EQ(dot, takt::to_dot(graph));
    EXPECT_NE(dot.find("quoted \\\"graph\\\"\\\\line\\nnext\\tcolumn\\r"),
              std::string::npos);
    EXPECT_NE(dot.find("g_n0 -> g_n1 [label=\"frames\\nout -> in\"]"),
              std::string::npos);
    EXPECT_EQ(dot.find("not-exported"), std::string::npos);
    EXPECT_NE(takt::to_dot(graph, {false, false}).find("rankdir=TB"),
              std::string::npos);
    graph.name = std::string(1, '\x01');
    EXPECT_THROW((void)takt::to_dot(graph), std::invalid_argument);
}

// Verifies empty export succeeds while invalid node/port references fail.
TEST(Graphviz,
     HandlesEmptyGraphsAndRejectsInvalidReferences)
{
    EXPECT_NE(takt::to_dot({}).find("digraph pipeline"), std::string::npos);
    auto graph = chain();
    graph.edges[0].to_node = "missing";
    EXPECT_THROW((void)takt::to_dot(graph), std::invalid_argument);
    graph = chain();
    graph.edges[0].to_port = "missing";
    EXPECT_THROW((void)takt::to_dot(graph), std::invalid_argument);
    graph = chain();
    graph.nodes.push_back(graph.nodes[0]);
    EXPECT_THROW((void)takt::to_dot(graph), std::invalid_argument);
    graph = chain();
    graph.nodes[0].name.clear();
    EXPECT_THROW((void)takt::to_dot(graph), std::invalid_argument);
}

// Verifies nested clusters keep input and output boundary identities distinct.
TEST(Graphviz,
     ExpandsNestedClustersWithDistinctInputAndOutputBoundaries)
{
    auto graph = chain();
    auto inner = chain();
    auto nested = chain();
    inner.nodes[1].output_ports = {{"out", takt::PortDirection::Output, {}}};
    nested.input_mappings = {{"in", {"sink", "in"}}};
    nested.output_mappings = {{"out", {"source", "out"}}};
    inner.subgraphs.emplace("sink", nested);
    graph.nodes[1].output_ports = {{"result", takt::PortDirection::Output, {}}};
    inner.input_mappings = {{"in", {"sink", "in"}}};
    inner.output_mappings = {{"result", {"sink", "out"}}};
    graph.subgraphs.emplace("sink", inner);
    const auto dot = takt::to_dot(graph);
    EXPECT_NE(dot.find("subgraph cluster_g_n1 {"), std::string::npos);
    EXPECT_NE(dot.find("subgraph cluster_g_n1_n1 {"), std::string::npos);
    EXPECT_NE(dot.find("g_n0 -> g_n1_in0"), std::string::npos);
    EXPECT_NE(dot.find("g_n1_in0 -> g_n1_n1_in0"), std::string::npos);
    EXPECT_NE(dot.find("g_n1_n1_out0 -> g_n1_out0"), std::string::npos);
    const auto collapsed = takt::to_dot(graph, {false, true});
    EXPECT_EQ(collapsed.find("cluster_"), std::string::npos);
    EXPECT_NE(collapsed.find("shape=box3d"), std::string::npos);
    graph.subgraphs["sink"].input_mappings["missing"] = {"sink", "in"};
    EXPECT_THROW((void)takt::to_dot(graph), std::invalid_argument);
}

// Verifies descriptive graph export never requires execution bindings.
TEST(Graphviz,
     BuilderExportsUnboundGraphWithoutStartingRuntime)
{
    auto builder = takt::PipelineBuilder::create("dot-unbound");
    builder.add_node("source").add_output_port("source", "out");
    builder.add_node("sink").add_input_port("sink", "in");
    builder.connect("source", "out", "sink", "in", {}, "frames");
    EXPECT_NE(builder.to_dot().find("frames"), std::string::npos);
    EXPECT_THROW((void)builder.build_runtime(), std::logic_error);
    auto snapshot = builder.graph();
    builder.add_node("later");
    EXPECT_EQ(snapshot.nodes.size(), 2U);
    EXPECT_EQ(builder.graph().nodes.size(), 3U);
}

// Verifies rebinding removes stale nested topology snapshots.
TEST(Graphviz,
     BoundSubgraphSnapshotMatchesBindingAndRebindingClearsIt)
{
    auto inner = takt::PipelineBuilder::create("dot-bound-inner");
    inner.add_node("worker").bind_node("worker", std::make_shared<IdleNode>());
    auto outer = takt::PipelineBuilder::create("dot-bound-outer");
    outer.add_node("group").bind_subgraph("group", inner);
    EXPECT_NE(outer.to_dot().find("worker"), std::string::npos);
    inner.add_node("after-binding");
    EXPECT_EQ(outer.to_dot().find("after-binding"), std::string::npos);
    outer.bind_node("group", std::make_shared<IdleNode>());
    EXPECT_EQ(outer.to_dot().find("cluster_"), std::string::npos);
}
