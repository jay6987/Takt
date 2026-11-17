// Verifies node port metadata, ordered pipelines, and nested runtime startup.
#include "node_tests_test_support.h"

// Verifies node context retains all declared input and output names.
TEST(NodeTests,
     NodeBaseStoresMultiplePipeNames)
{
    /// Represents the probe agent test fixture.
    struct ProbeAgent final : takt::Node
    {
        // Creates an inert node whose port context can be inspected.
        ProbeAgent() : takt::Node("probe") {}

        /// Represents the worker task test fixture.
        struct WorkerTask final : public takt::Node::Task
        {
        };

        // Produces one empty task for each configured worker.
        std::unique_ptr<takt::Node::Task>
        build_task(uint64_t sequence_id) override
        {
            if (sequence_id >= worker_count())
            {
                return nullptr;
            }
            return std::make_unique<WorkerTask>();
        }

        // Leaves payloads unchanged because this probe only checks metadata.
        void process_task(size_t,
                          takt::Node::Task&) override
        {
        }
    };

    ProbeAgent node;
    node.clear_input_port_names();
    node.clear_output_port_names();
    node.add_input_port_name("in-1");
    node.add_input_port_name("in-2");
    node.add_output_port_name("out-1");
    node.add_output_port_name("out-2");

    ASSERT_EQ(node.input_port_names().size(), 2U);
    ASSERT_EQ(node.output_port_names().size(), 2U);
    EXPECT_EQ(node.input_port_names()[0], "in-1");
    EXPECT_EQ(node.input_port_names()[1], "in-2");
    EXPECT_EQ(node.output_port_names()[0], "out-1");
    EXPECT_EQ(node.output_port_names()[1], "out-2");
}

// Verifies staged concurrent processing reconstructs ordered output frames.
TEST(NodeTests,
     MultiStageNodesReconstructOrderedOutput)
{
    const std::string slogan = "We will rock you!";
    const std::vector<size_t> shuffled_order = {2, 3,  8,  4, 0,  11, 5,  6, 7,
                                                1, 12, 13, 9, 10, 15, 16, 14};

    takt::Pipe<IndexedChar> in("ordered-in");
    takt::Pipe<char> out("ordered-out");
    configure_pipe(in, 32, IndexedChar{});
    configure_pipe(out, 32, '\0');
    in.initialize();
    out.initialize();
    OrderedCharStore store;

    IngressReorderAgent stage1(in, store);
    OrderedEmitAgent stage2(store, out);

    stage1.start();
    stage2.start();

    for (size_t idx : shuffled_order)
    {
        auto w = in.acquire_shot_end_write(1);
        w.value() = IndexedChar{idx, slogan[idx]};
        w.publish();
    }
    in.close();

    stage1.join();
    stage2.join();

    std::string reconstructed;
    while (true)
    {
        try
        {
            auto r = out.acquire_read();
            reconstructed.push_back(r.value());
        }
        catch (const takt::PipeClosedAndEmptySignal&)
        {
            break;
        }
    }

    EXPECT_EQ(reconstructed, slogan);
}

// Verifies composite node execution starts and joins the nested runtime.
TEST(NodeTests,
     SubgraphNodeStartsAndJoinsInnerRuntime)
{
    std::atomic<int> run_count{0};
    std::vector<std::shared_ptr<takt::NodeBase>> inner_nodes;
    inner_nodes.push_back(std::make_shared<CountingNode>(run_count));
    inner_nodes.push_back(std::make_shared<CountingNode>(run_count));

    auto inner_runtime =
        std::make_shared<takt::PipelineRuntime>(std::move(inner_nodes));
    takt::SubgraphNode subgraph("sg", inner_runtime);

    inner_runtime->initialize();
    subgraph.start();
    subgraph.join();

    EXPECT_EQ(run_count.load(std::memory_order_relaxed), 2);
}
