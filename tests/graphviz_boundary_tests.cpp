// Verifies exported subgraph boundaries and direct cross-cluster connections.
#include "graphviz_tests_test_support.h"

// Verifies bound logical port mappings remain visible in graph export.
TEST(Graphviz,
     BuilderRetainsBoundPortMappingsInExport)
{
    auto outer_input = std::make_shared<takt::Pipe<int>>("dot-outer-in");
    auto outer_output = std::make_shared<takt::Pipe<int>>("dot-outer-out");
    auto inner_input = std::make_shared<takt::Pipe<int>>("dot-inner-in");
    auto inner_output = std::make_shared<takt::Pipe<int>>("dot-inner-out");
    auto inner = takt::PipelineBuilder::create("dot-mapped-inner");
    inner.add_node("worker")
        .add_input_port(
            "worker", "input",
            {{"pipe_endpoint", takt::make_pipe_endpoint(inner_input)}})
        .add_output_port(
            "worker", "output",
            {{"pipe_endpoint", takt::make_pipe_endpoint(inner_output)}})
        .bind_node("worker", std::make_shared<IdleNode>());
    auto outer = takt::PipelineBuilder::create("dot-mapped-outer");
    outer.add_node("filter")
        .add_input_port(
            "filter", "raw",
            {{"pipe_endpoint", takt::make_pipe_endpoint(outer_input)}})
        .add_output_port(
            "filter", "result",
            {{"pipe_endpoint", takt::make_pipe_endpoint(outer_output)}})
        .bind_subgraph("filter", inner, {{"raw", {"worker", "input"}}},
                       {{"result", {"worker", "output"}}});
    const auto dot = outer.to_dot();
    EXPECT_NE(dot.find("g_n0_in0_bridge -> g_n0_n0"), std::string::npos);
    EXPECT_NE(dot.find("g_n0_n0 -> g_n0_out0_bridge"), std::string::npos);
    EXPECT_NE(dot.find("dedicated worker"), std::string::npos);
    EXPECT_EQ(dot.find("shape=ellipse"), std::string::npos);
    EXPECT_NE(dot.find("raw -> input"), std::string::npos);
    EXPECT_NE(dot.find("output -> result"), std::string::npos);
}

// Verifies direct connections cross nested clusters without invented workers.
TEST(Graphviz,
     DirectConnectionsCrossClusterWithoutArtificialWorkers)
{
    const auto graph = mapped_graph(takt::MappingTransport::Direct,
                                    takt::MappingTransport::Direct);
    const auto dot = takt::to_dot(graph);
    EXPECT_NE(dot.find("g_n0 -> g_n1_n0"), std::string::npos);
    EXPECT_NE(dot.find("g_n1_n0 -> g_n2"), std::string::npos);
    EXPECT_NE(dot.find("subgraph cluster_g_n1"), std::string::npos);
    EXPECT_NE(dot.find("direct/shared Pipe: input"), std::string::npos);
    EXPECT_EQ(dot.find("_bridge"), std::string::npos);
    EXPECT_EQ(dot.find("shape=ellipse"), std::string::npos);
    const auto interfaces =
        takt::to_dot(graph, {true, true, takt::DotView::Interfaces});
    EXPECT_NE(interfaces.find("g_n0 -> g_n1_in0"), std::string::npos);
    EXPECT_NE(interfaces.find("g_n1_in0 -> g_n1_n0"), std::string::npos);
    EXPECT_NE(interfaces.find("direct/shared Pipe: input"), std::string::npos);
    EXPECT_NE(takt::to_dot(graph, {false, true})
                  .find("direct=2 bridge=0 unresolved=0"),
              std::string::npos);
    EXPECT_NE(takt::to_dot(graph, {true, false}).find("rankdir=TB"),
              std::string::npos);
}
