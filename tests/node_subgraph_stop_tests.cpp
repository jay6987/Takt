// Verifies subgraph cancellation propagates to its inner runtime.
#include "node_tests_test_support.h"

// Verifies composite cancellation reaches nested processing nodes.
TEST(NodeTests,
     SubgraphNodeRequestStopPropagatesToInnerRuntime)
{
    std::promise<void> worker_started_promise;
    auto worker_started = worker_started_promise.get_future();
    std::atomic<bool> observed_stop{false};
    std::vector<std::shared_ptr<takt::NodeBase>> inner_nodes;
    inner_nodes.push_back(
        std::make_shared<StoppableNode>(worker_started_promise, observed_stop));

    auto inner_runtime =
        std::make_shared<takt::PipelineRuntime>(std::move(inner_nodes));
    takt::SubgraphNode subgraph("sg-stop", inner_runtime);

    inner_runtime->initialize();
    subgraph.start();
    ASSERT_EQ(worker_started.wait_for(std::chrono::seconds(2)),
              std::future_status::ready);
    subgraph.request_stop();
    subgraph.join();

    EXPECT_TRUE(observed_stop.load(std::memory_order_relaxed));
}
