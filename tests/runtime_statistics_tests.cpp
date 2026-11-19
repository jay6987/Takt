#include "takt/pipeline/node.h"
#include "takt/pipeline/pipe.h"
#include "takt/pipeline/pipeline_runtime.h"
#include "takt/pipeline/subgraph_node.h"

#include <gtest/gtest.h>

#include <chrono>
#include <future>
#include <memory>
#include <stdexcept>
#include <thread>

namespace
{
using namespace std::chrono_literals;

class CountingNode final : public takt::Node
{
  public:
    explicit CountingNode(bool fail = false, bool single_task_rounds = false)
        : Node("count", 3), fail_(fail), single_task_rounds_(single_task_rounds)
    {
    }

  private:
    std::unique_ptr<Task> build_task(uint64_t sequence) override
    {
        if (sequence >= 5)
            return nullptr;
        auto task = std::make_unique<Task>();
        task->barrier = single_task_rounds_ || sequence == 1 || sequence == 4;
        return task;
    }

    void process_task(size_t, Task& task) override
    {
        if (task.sequence_id == 1)
            std::this_thread::sleep_for(1ms);
    }

    void on_task_done(size_t, const Task& task) override
    {
        if (fail_ && task.sequence_id == 1)
            throw std::runtime_error("task completion failed");
    }

    bool fail_;
    bool single_task_rounds_;
};

std::shared_ptr<takt::Pipe<int>> make_pipe()
{
    auto pipe = std::make_shared<takt::Pipe<int>>("frames");
    pipe->set_producer({"source"}, 0);
    pipe->set_consumer({"sink"});
    pipe->set_slot_capacity(1);
    return pipe;
}

void publish(takt::Pipe<int>& pipe)
{
    auto write = pipe.acquire_shot_end_write(1);
    write.publish();
}

TEST(RuntimeStatistics, DisabledRuntimeReturnsEmptySnapshot)
{
    takt::PipelineRuntime runtime({std::make_shared<CountingNode>()}, {make_pipe()});
    runtime.initialize();
    runtime.start();
    runtime.join();
    const auto snapshot = runtime.statistics();
    EXPECT_TRUE(snapshot.nodes.empty());
    EXPECT_TRUE(snapshot.pipes.empty());
}

TEST(RuntimeStatistics, CountsWorkersAndReportsSharedNestedPipeOnce)
{
    auto pipe = make_pipe();
    auto child = std::make_shared<takt::PipelineRuntime>(
        std::vector<std::shared_ptr<takt::NodeBase>>{std::make_shared<CountingNode>()},
        std::vector<std::shared_ptr<takt::PipeBase>>{pipe});
    auto subgraph = std::make_shared<takt::SubgraphNode>("nested", child);
    takt::PipelineRuntime runtime({subgraph}, {pipe});
    runtime.initialize();
    runtime.enable_statistics();
    EXPECT_THROW(runtime.statistics(), std::logic_error);
    runtime.start();
    EXPECT_THROW(runtime.statistics(), std::logic_error);
    runtime.join();

    const auto result = runtime.statistics();
    ASSERT_EQ(result.nodes.size(), 1U);
    ASSERT_EQ(result.pipes.size(), 1U);
    const auto& node = result.nodes.front();
    EXPECT_EQ(node.path, "nested/count");
    EXPECT_EQ(result.pipes.front().path, "nested/frames");
    ASSERT_EQ(node.workers.size(), 3U);
    uint64_t tasks = 0;
    for (const auto& worker : node.workers)
    {
        tasks += worker.tasks_succeeded;
        EXPECT_EQ(worker.tasks_started, worker.tasks_succeeded);
        EXPECT_EQ(worker.tasks_failed, 0U);
    }
    EXPECT_EQ(tasks, 5U);
    EXPECT_GT(node.ingress.worker_wait_time, 0ns);
    EXPECT_GT(node.ingress.first_task_build_time, 0ns);
    EXPECT_GT(node.ingress.in_round_task_build_time, 0ns);
    EXPECT_EQ(node.ingress.task_build_time,
              node.ingress.first_task_build_time +
                  node.ingress.in_round_task_build_time);
    EXPECT_GT(node.ingress.drain_wait_time, 0ns);
}

TEST(RuntimeStatistics, StartsNewBuildRoundAfterEachBarrier)
{
    takt::PipelineRuntime runtime({std::make_shared<CountingNode>(false, true)});
    runtime.initialize();
    runtime.enable_statistics();
    runtime.start();
    runtime.join();

    const auto result = runtime.statistics();
    ASSERT_EQ(result.nodes.size(), 1U);
    const auto& ingress = result.nodes.front().ingress;
    EXPECT_GT(ingress.first_task_build_time, 0ns);
    EXPECT_EQ(ingress.in_round_task_build_time, 0ns);
    EXPECT_EQ(ingress.task_build_time, ingress.first_task_build_time);
}

TEST(RuntimeStatistics, RetainsTaskFailureAfterJoinReportsIt)
{
    takt::PipelineRuntime runtime({std::make_shared<CountingNode>(true)});
    runtime.initialize();
    runtime.enable_statistics();
    runtime.start();
    EXPECT_THROW(runtime.join(), std::runtime_error);
    const auto result = runtime.statistics();
    ASSERT_EQ(result.nodes.size(), 1U);
    uint64_t failed = 0;
    for (const auto& worker : result.nodes.front().workers)
    {
        EXPECT_EQ(worker.tasks_started, worker.tasks_succeeded + worker.tasks_failed);
        if (worker.tasks_failed != 0)
            EXPECT_GT(worker.work_time, 0ns);
        failed += worker.tasks_failed;
    }
    EXPECT_EQ(failed, 1U);
}

TEST(RuntimeStatistics, DistinguishesPipeBlockingFromReadyOperations)
{
    auto pipe = make_pipe();
    takt::PipelineRuntime runtime({}, {pipe});
    runtime.initialize();
    runtime.enable_statistics();
    runtime.start();

    publish(*pipe);
    auto held = pipe->acquire_read();
    auto writer = std::async(std::launch::async, [&] { publish(*pipe); });
    EXPECT_EQ(writer.wait_for(100ms), std::future_status::timeout);
    held = {};
    writer.get();
    (void)pipe->acquire_read();

    auto reader = std::async(std::launch::async, [&]
    {
        EXPECT_THROW(pipe->acquire_read(), takt::PipeClosedAndEmptySignal);
    });
    EXPECT_EQ(reader.wait_for(100ms), std::future_status::timeout);
    runtime.request_stop();
    reader.get();
    runtime.join();

    const auto result = runtime.statistics();
    ASSERT_EQ(result.pipes.size(), 1U);
    const auto& stats = result.pipes.front();
    EXPECT_EQ(stats.slot_capacity, 1U);
    EXPECT_EQ(stats.peak_unreusable_slots, 1U);
    EXPECT_EQ(stats.write_wait.count, 1U);
    EXPECT_GT(stats.write_wait.time, 0ns);
    EXPECT_EQ(stats.read_wait.count, 1U);
    EXPECT_GT(stats.read_wait.time, 0ns);
}
} // namespace
