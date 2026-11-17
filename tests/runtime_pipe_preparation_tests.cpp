// Verifies nested bridge pipe preparation and retained overlap frame slots.
#include "runtime_initialization_tests_test_support.h"

// Verifies both bridge sides are prepared with the nested runtime tree.
TEST(RuntimeInitialization,
     BridgePipesAreIncludedInInnerRuntimePreparation)
{
    auto source = std::make_shared<takt::Pipe<int>>("outer");
    auto target = std::make_shared<takt::Pipe<int>>("inner");
    for (const auto& pipe : {source, target})
    {
        pipe->set_producer(
            {"producer", takt::concurrency(1), takt::frames_per_write(1)}, 0);
        pipe->set_consumer({"consumer", takt::concurrency(1),
                            takt::frames_per_read(1), takt::overlap_frames(0)});
    }
    auto inner = std::make_shared<takt::PipelineRuntime>(
        std::vector<std::shared_ptr<takt::NodeBase>>{});
    auto subgraph = std::make_shared<takt::SubgraphNode>(
        "subgraph", inner,
        std::unordered_map<std::string, takt::NodePortRef>{
            {"in", {"node", "in"}}},
        std::unordered_map<std::string, takt::NodePortRef>{},
        std::vector<takt::SubgraphNode::BridgeLink>{
            {takt::make_pipe_endpoint(source), takt::make_pipe_endpoint(target),
             "outer.in", "inner.in"}});
    takt::PipelineRuntime root({subgraph}, {source});
    root.initialize();
    EXPECT_TRUE(inner->initialized());
    EXPECT_TRUE(source->initialized());
    EXPECT_TRUE(target->initialized());
}

// Verifies configured overlap exposes retained circular-buffer frames.
TEST(RuntimeInitialization,
     ConfiguredOverlapReadsRetainedRingSlots)
{
    takt::Pipe<int> pipe("overlap");
    pipe.set_producer(
        {"source", takt::concurrency(1), takt::frames_per_write(3)}, 0);
    pipe.set_consumer({"sink", takt::concurrency(1), takt::frames_per_read(2),
                       takt::overlap_frames(1)});
    pipe.set_slot_capacity(3);
    pipe.initialize();
    {
        auto write = pipe.acquire_shot_end_write(3);
        write.value(0) = 1;
        write.value(1) = 2;
        write.value(2) = 3;
        write.publish();
    }
    {
        auto read = pipe.acquire_read();
        ASSERT_EQ(read.frame_count(), 2U);
        EXPECT_EQ(read.overlap_frames(), 0U);
        EXPECT_EQ(read.value(0), 1);
        EXPECT_EQ(read.value(1), 2);
    }
    {
        auto read = pipe.acquire_read();
        ASSERT_EQ(read.frame_count(), 2U);
        EXPECT_EQ(read.overlap_frames(), 1U);
        EXPECT_EQ(read.value(0), 2);
        EXPECT_EQ(read.value(1), 3);
    }
}
