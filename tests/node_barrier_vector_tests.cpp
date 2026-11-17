// Verifies vector regions share tokens and gate subsequent frame acquisition.
#include "node_barrier_tests_test_support.h"

namespace
{
using VectorPipe = takt::Pipe<std::vector<float>>;

// Configures and initializes the shared-vector boundary pipes.
void initialize_vector_pipes(const std::shared_ptr<VectorPipe>& input,
                             const std::shared_ptr<VectorPipe>& output)
{
    input->set_producer(
        {"source", takt::concurrency(1), takt::frames_per_write(1)},
        std::vector<float>(8, 0.0F));
    input->set_consumer({"vector-node", takt::concurrency(1),
                         takt::frames_per_read(1), takt::overlap_frames(0)});
    input->set_slot_capacity(2);
    input->initialize();
    output->set_producer(
        {"vector-node", takt::concurrency(2), takt::frames_per_write(1)},
        std::vector<float>(8, 0.0F));
    output->set_consumer({"sink", takt::concurrency(1),
                          takt::frames_per_read(1), takt::overlap_frames(0)});
    output->set_slot_capacity(2);
    output->initialize();
}

// Publishes two distinguishable vector frames before closing input.
void publish_vector_inputs(const std::shared_ptr<VectorPipe>& input)
{
    for (size_t group = 0; group < 2; ++group)
    {
        auto write = input->acquire_shot_end_write(1);
        auto& values = write.value(0);
        for (size_t i = 0; i < values.size(); ++i)
            values[i] = static_cast<float>(group * 10 + i);
        write.publish();
    }
    input->close();
}

// Releases each group only after both regions arrive and ordering is checked.
void release_vector_groups(VectorBarrierProbe& node)
{
    for (size_t group = 0; group < 2; ++group)
    {
        std::unique_lock<std::mutex> lock(node.gate_mutex);
        EXPECT_TRUE(node.gate_changed.wait_for(
            lock, std::chrono::seconds(5),
            [&] { return node.entered[group][0] && node.entered[group][1]; }));
        EXPECT_EQ(node.acquired.load(), group + 1);
        EXPECT_EQ(node.published.load(), group);
        EXPECT_EQ(node.released.load(), group);
        node.release_group[group] = true;
        lock.unlock();
        node.gate_changed.notify_all();
    }
}

// Confirms output closure and each frame's expected transformed elements.
void verify_vector_outputs(const std::shared_ptr<VectorPipe>& output)
{
    EXPECT_TRUE(output->closed());
    for (size_t group = 0; group < 2; ++group)
    {
        auto read = output->acquire_read();
        for (size_t i = 0; i < 8; ++i)
            EXPECT_FLOAT_EQ(read.value(0)[i],
                            static_cast<float>(group * 110 + i));
    }
}
} // namespace

// Keep vector tokens on ingress and publish only after parallel regions drain.
TEST(NodeBarrier,
     VectorRegionsShareTokensAndGateTheNextFrameOnBarrierCleanup)
{
    auto input =
        std::make_shared<takt::Pipe<std::vector<float>>>("vector-input");
    auto output =
        std::make_shared<takt::Pipe<std::vector<float>>>("vector-output");
    initialize_vector_pipes(input, output);
    publish_vector_inputs(input);

    VectorBarrierProbe node(input, output);
    node.start();
    release_vector_groups(node);
    node.join();

    EXPECT_EQ(node.failure(), nullptr);
    EXPECT_TRUE(node.ordering_valid.load());
    EXPECT_EQ(node.acquired.load(), 2U);
    EXPECT_EQ(node.published.load(), 2U);
    EXPECT_EQ(node.released.load(), 2U);
    verify_vector_outputs(output);
}
