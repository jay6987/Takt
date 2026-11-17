// Verifies nested DOT transport labels, aliases, and bridge resolution.
#include "graphviz_tests_test_support.h"

// Verifies only bound bridges claim workers and unresolved maps stay explicit.
TEST(Graphviz,
     MixedMappingsShowExactlyOneBridgeAndUnresolvedNeverClaimsWorker)
{
    auto graph = mapped_graph(takt::MappingTransport::Direct,
                              takt::MappingTransport::Bridge);
    const auto dot = takt::to_dot(graph);
    EXPECT_NE(dot.find("g_n0 -> g_n1_n0"), std::string::npos);
    EXPECT_NE(dot.find("g_n1_n0 -> g_n1_out0_bridge"), std::string::npos);
    EXPECT_NE(dot.find("g_n1_out0_bridge -> g_n2"), std::string::npos);
    EXPECT_NE(dot.find("inner-output -> output"), std::string::npos);
    const auto worker = dot.find("dedicated worker");
    ASSERT_NE(worker, std::string::npos);
    EXPECT_EQ(dot.find("dedicated worker", worker + 1), std::string::npos);
    const auto interfaces =
        takt::to_dot(graph, {true, true, takt::DotView::Interfaces});
    EXPECT_NE(interfaces.find("g_n1_out0_bridge -> g_n1_out0"),
              std::string::npos);
    graph.subgraphs.at("group").input_transports.clear();
    graph.subgraphs.at("group").output_transports.clear();
    const auto unresolved = takt::to_dot(graph);
    EXPECT_NE(unresolved.find("unresolved"), std::string::npos);
    EXPECT_NE(unresolved.find("style=dashed"), std::string::npos);
    EXPECT_EQ(unresolved.find("dedicated worker"), std::string::npos);
    EXPECT_EQ(unresolved.find("direct/shared"), std::string::npos);
}

// Verifies nested direct resolution reaches leaves and retains unused ports.
TEST(Graphviz,
     NestedDirectResolvesLeafAndPreservesUnconnectedPorts)
{
    auto graph = mapped_graph(takt::MappingTransport::Direct,
                              takt::MappingTransport::Direct);
    auto& inner = graph.subgraphs.at("group");
    auto leaf = inner;
    inner.subgraphs.emplace("worker", leaf);
    const auto dot = takt::to_dot(graph);
    EXPECT_NE(dot.find("g_n0 -> g_n1_n0_n0"), std::string::npos);
    EXPECT_NE(dot.find("g_n1_n0_n0 -> g_n2"), std::string::npos);
    EXPECT_EQ(dot.find("shape=ellipse"), std::string::npos);
    graph.edges.clear();
    const auto unconnected = takt::to_dot(graph);
    EXPECT_NE(unconnected.find("g_n1_in0 -> g_n1_n0_n0"), std::string::npos);
    EXPECT_NE(unconnected.find("g_n1_n0_n0 -> g_n1_out0"), std::string::npos);
    EXPECT_NE(unconnected.find("shape=ellipse"), std::string::npos);
    EXPECT_EQ(unconnected, takt::to_dot(graph));
}

// Verifies inconsistent transport is rejected and pipe labels are DOT-escaped.
TEST(Graphviz,
     TransportMetadataIsCheckedAndPipeNamesAreEscaped)
{
    auto graph = mapped_graph(takt::MappingTransport::Direct,
                              takt::MappingTransport::Bridge);
    auto& inner = graph.subgraphs.at("group");
    inner.input_transports["in"] = {takt::MappingTransport::Direct,
                                    "quoted \"pipe\"\nline",
                                    "quoted \"pipe\"\nline"};
    EXPECT_NE(takt::to_dot(graph).find("quoted \\\"pipe\\\"\\nline"),
              std::string::npos);
    inner.input_transports.emplace("missing", takt::MappingTransportInfo{});
    EXPECT_THROW(takt::to_dot(graph), std::invalid_argument);
    inner.input_transports.erase("missing");
    inner.input_transports["in"].target_pipe = "other";
    EXPECT_THROW(takt::to_dot(graph), std::invalid_argument);
    inner.input_transports["in"].mode = static_cast<takt::MappingTransport>(99);
    EXPECT_THROW(takt::to_dot(graph), std::invalid_argument);
    EXPECT_THROW(
        takt::to_dot(graph, {true, true, static_cast<takt::DotView>(99)}),
        std::invalid_argument);
}

// Verifies nested endpoint resolution stops at real bridges and keeps aliases.
TEST(Graphviz,
     NestedDirectStopsAtBridgeAndMappedAliasesRemainDistinct)
{
    auto graph = mapped_graph(takt::MappingTransport::Direct,
                              takt::MappingTransport::Direct);
    auto& inner = graph.subgraphs.at("group");
    auto leaf = inner;
    leaf.input_transports["in"] = {takt::MappingTransport::Bridge, "input",
                                   "inner-input"};
    inner.subgraphs.emplace("worker", leaf);
    const auto dot = takt::to_dot(graph);
    EXPECT_NE(dot.find("g_n0 -> g_n1_n0_in0_bridge"), std::string::npos);
    EXPECT_NE(dot.find("g_n1_n0_in0_bridge -> g_n1_n0_n0"), std::string::npos);
    EXPECT_EQ(dot.find("g_n0 -> g_n1_n0_n0"), std::string::npos);

    auto aliases = mapped_graph(takt::MappingTransport::Bridge,
                                takt::MappingTransport::Bridge);
    auto& group = aliases.nodes[1];
    group.input_ports.push_back({"alias", takt::PortDirection::Input, {}});
    auto& mapped = aliases.subgraphs.at("group");
    mapped.input_mappings["alias"] = {"worker", "in"};
    mapped.input_transports["alias"] = {takt::MappingTransport::Bridge,
                                        "other-input", "inner-input"};
    const auto alias_dot = takt::to_dot(aliases);
    EXPECT_NE(alias_dot.find("g_n1_in0_bridge -> g_n1_n0"), std::string::npos);
    EXPECT_NE(alias_dot.find("g_n1_in1_bridge -> g_n1_n0"), std::string::npos);
}
