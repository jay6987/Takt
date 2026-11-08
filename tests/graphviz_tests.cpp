// Verifies DOT escaping, topology views and bound subgraph transport labels.
#include "takt/takt.h"

#include <gtest/gtest.h>

#include <memory>
#include <stdexcept>
#include <string>

namespace
{
// Returns a minimal valid source-to-sink graph for DOT variations.
takt::PipelineGraph chain()
{
    takt::PipelineGraph graph;
    graph.name = "test";
    graph.nodes = {
        {"source", {}, {}, {{"out", takt::PortDirection::Output, {}}}},
        {"sink", {}, {{"in", takt::PortDirection::Input, {}}}, {}}};
    graph.edges = {{"frames", "source", "out", "sink", "in", {}}};
    return graph;
}

class IdleNode final : public takt::Node
{
  public:
    // Creates an inert executable binding for graph-export tests.
    IdleNode() : Node("idle") {}

  private:
    // Ends ingress immediately without producing processing work.
    std::unique_ptr<Task> build_task(uint64_t) override
    {
        return nullptr;
    }
    // Performs no processing because the fixture never creates tasks.
    void process_task(size_t,
                      Task&) override
    {
    }
};
} // namespace

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

namespace
{
// Builds a nested topology with independently selectable boundary transports.
takt::PipelineGraph mapped_graph(takt::MappingTransport input,
                                 takt::MappingTransport output)
{
    takt::PipelineGraph inner;
    inner.name = "inner";
    inner.nodes = {{"worker",
                    {},
                    {{"in", takt::PortDirection::Input, {}}},
                    {{"out", takt::PortDirection::Output, {}}}}};
    inner.input_mappings = {{"in", {"worker", "in"}}};
    inner.output_mappings = {{"out", {"worker", "out"}}};
    inner.input_transports = {
        {"in",
         {input, "input",
          input == takt::MappingTransport::Direct ? "input" : "inner-input"}}};
    inner.output_transports = {
        {"out",
         {output,
          output == takt::MappingTransport::Direct ? "output" : "inner-output",
          "output"}}};
    takt::PipelineGraph graph;
    graph.name = "mapped";
    graph.nodes = {
        {"source", {}, {}, {{"out", takt::PortDirection::Output, {}}}},
        {"group",
         {},
         {{"in", takt::PortDirection::Input, {}}},
         {{"out", takt::PortDirection::Output, {}}}},
        {"sink", {}, {{"in", takt::PortDirection::Input, {}}}, {}}};
    graph.edges = {{"input", "source", "out", "group", "in", {}},
                   {"output", "group", "out", "sink", "in", {}}};
    graph.subgraphs = {{"group", inner}};
    return graph;
}
} // namespace

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
