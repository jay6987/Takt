#include "takt/takt.h"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <map>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

namespace
{
class SingleWorkerDoubleAgent final : public takt::Node
{
  public:
    SingleWorkerDoubleAgent(takt::Pipe<int>& in, takt::Pipe<int>& out)
        : takt::Node("single-worker-double"), in_(in), out_(out)
    {
    }

  protected:
    struct WorkerTask final : public takt::Node::Task
    {
    };

    std::unique_ptr<takt::Node::Task> build_task(uint64_t sequence_id) override
    {
        if (sequence_id >= worker_count())
        {
            return nullptr;
        }
        return std::make_unique<WorkerTask>();
    }

    void process_task(size_t, takt::Node::Task&) override
    {
        while (true)
        {
            try
            {
                auto input = in_.acquire_read();
                auto output = out_.acquire_write();
                output.value() = input.value() * 2;
                output.publish();
            }
            catch (const takt::PipeClosedAndEmptySignal&)
            {
                out_.close();
                break;
            }
        }
    }

  private:
    takt::Pipe<int>& in_;
    takt::Pipe<int>& out_;
};

class MultiWorkerSumAgent final : public takt::Node
{
  public:
    MultiWorkerSumAgent(takt::Pipe<int>& in, std::atomic<int>& sum, size_t workers)
        : takt::Node("multi-worker-sum", workers), in_(in), sum_(sum)
    {
    }

  protected:
    struct WorkerTask final : public takt::Node::Task
    {
    };

    std::unique_ptr<takt::Node::Task> build_task(uint64_t sequence_id) override
    {
        if (sequence_id >= worker_count())
        {
            return nullptr;
        }
        return std::make_unique<WorkerTask>();
    }

    void process_task(size_t, takt::Node::Task&) override
    {
        while (true)
        {
            try
            {
                auto token = in_.acquire_read();
                sum_.fetch_add(token.value(), std::memory_order_relaxed);
            }
            catch (const takt::PipeClosedAndEmptySignal&)
            {
                break;
            }
        }
    }

  private:
    takt::Pipe<int>& in_;
    std::atomic<int>& sum_;
};

class FaultySingleWorkerAgent final : public takt::Node
{
  public:
        FaultySingleWorkerAgent() : takt::Node("faulty-single-worker") {}

    const takt::NodeExceptionContext& captured() const
    {
        return captured_;
    }

    bool has_capture() const
    {
        return has_capture_;
    }

  protected:
    struct WorkerTask final : public takt::Node::Task
    {
    };

    std::unique_ptr<takt::Node::Task> build_task(uint64_t sequence_id) override
    {
        if (sequence_id >= worker_count())
        {
            return nullptr;
        }
        return std::make_unique<WorkerTask>();
    }

    void process_task(size_t, takt::Node::Task&) override
    {
        throw std::runtime_error("boom");
    }

    void on_exception(const takt::NodeExceptionContext& context) override
    {
        captured_ = context;
        has_capture_ = true;
        takt::Node::on_exception(context);
    }

  private:
    takt::NodeExceptionContext captured_;
    bool has_capture_ = false;
};

class FaultyMultiWorkerAgent final : public takt::Node
{
  public:
        FaultyMultiWorkerAgent() : takt::Node("faulty-multi-worker", 2) {}

    const takt::NodeExceptionContext& captured() const
    {
        return captured_;
    }

    bool has_capture() const
    {
        return has_capture_;
    }

  protected:
    struct WorkerTask final : public takt::Node::Task
    {
    };

    std::unique_ptr<takt::Node::Task> build_task(uint64_t sequence_id) override
    {
        if (sequence_id >= worker_count())
        {
            return nullptr;
        }
        return std::make_unique<WorkerTask>();
    }

    void process_task(size_t, takt::Node::Task&) override
    {
        throw std::runtime_error("kaboom");
    }

    void on_exception(const takt::NodeExceptionContext& context) override
    {
        captured_ = context;
        has_capture_ = true;
        takt::Node::on_exception(context);
    }

  private:
    takt::NodeExceptionContext captured_;
    bool has_capture_ = false;
};

class CapturingLogger final : public takt::ILogger
{
  public:
    void display(const std::string&) override {}
    void display(const std::wstring&) override {}
    void info(const std::string&) override {}
    void info(const std::wstring&) override {}
    void warn(const std::string&) override {}
    void warn(const std::wstring&) override {}
    void error(const std::string& message) override
    {
        std::lock_guard<std::mutex> lk(mu_);
        error_messages_.push_back(message);
    }
    void error(const std::wstring&) override {}
    void debug(const std::string&) override {}
    void debug(const std::wstring&) override {}
    bool debug_enabled() const override
    {
        return false;
    }

    bool has_error_substring(const std::string& needle) const
    {
        std::lock_guard<std::mutex> lk(mu_);
        for (const auto& message : error_messages_)
        {
            if (message.find(needle) != std::string::npos)
            {
                return true;
            }
        }
        return false;
    }

  private:
    mutable std::mutex mu_;
    std::vector<std::string> error_messages_;
};

class ScopedLoggerReset final
{
  public:
    ~ScopedLoggerReset()
    {
        takt::reset_logger();
    }
};

struct IndexedChar
{
    size_t index = 0;
    char value = '\0';
};

class OrderedCharStore
{
  public:
    void put(size_t idx, char c)
    {
        std::lock_guard<std::mutex> lk(mu_);
        store_[idx] = c;
        cv_.notify_all();
    }

    void mark_done()
    {
        std::lock_guard<std::mutex> lk(mu_);
        done_ = true;
        cv_.notify_all();
    }

    bool wait_pop(size_t idx, char& out)
    {
        std::unique_lock<std::mutex> lk(mu_);
        cv_.wait(lk, [&] { return done_ || store_.find(idx) != store_.end(); });

        auto it = store_.find(idx);
        if (it != store_.end())
        {
            out = it->second;
            store_.erase(it);
            return true;
        }
        return false;
    }

  private:
    std::mutex mu_;
    std::condition_variable cv_;
    std::map<size_t, char> store_;
    bool done_ = false;
};

class IngressReorderAgent final : public takt::Node
{
  public:
    IngressReorderAgent(takt::Pipe<IndexedChar>& in, OrderedCharStore& store)
        : takt::Node("ingress-reorder"), in_(in), store_(store)
    {
    }

  protected:
    struct WorkerTask final : public takt::Node::Task
    {
    };

    std::unique_ptr<takt::Node::Task> build_task(uint64_t sequence_id) override
    {
        if (sequence_id >= worker_count())
        {
            return nullptr;
        }
        return std::make_unique<WorkerTask>();
    }

    void process_task(size_t, takt::Node::Task&) override
    {
        try
        {
            while (true)
            {
                auto r = in_.acquire_read();
                store_.put(r.value().index, r.value().value);
            }
        }
        catch (const takt::PipeClosedAndEmptySignal&)
        {
            store_.mark_done();
        }
    }

  private:
    takt::Pipe<IndexedChar>& in_;
    OrderedCharStore& store_;
};

class OrderedEmitAgent final : public takt::Node
{
  public:
    OrderedEmitAgent(OrderedCharStore& store, takt::Pipe<char>& out)
        : takt::Node("ordered-emit"), store_(store), out_(out)
    {
    }

  protected:
    struct WorkerTask final : public takt::Node::Task
    {
    };

    std::unique_ptr<takt::Node::Task> build_task(uint64_t sequence_id) override
    {
        if (sequence_id >= worker_count())
        {
            return nullptr;
        }
        return std::make_unique<WorkerTask>();
    }

    void process_task(size_t, takt::Node::Task&) override
    {
        size_t expected = 0;
        while (true)
        {
            char c = '\0';
            if (!store_.wait_pop(expected, c))
            {
                out_.close();
                break;
            }
            auto w = out_.acquire_write(true);
            w.value() = c;
            w.publish();
            ++expected;
        }
    }

  private:
    OrderedCharStore& store_;
    takt::Pipe<char>& out_;
};

class CountingNode final : public takt::Node
{
  public:
    explicit CountingNode(std::atomic<int>& count)
        : takt::Node("counting-node"), count_(count)
    {
    }

  protected:
    struct WorkerTask final : public takt::Node::Task
    {
    };

    std::unique_ptr<takt::Node::Task> build_task(uint64_t sequence_id) override
    {
        if (sequence_id >= worker_count())
        {
            return nullptr;
        }
        return std::make_unique<WorkerTask>();
    }

    void process_task(size_t, takt::Node::Task&) override
    {
        count_.fetch_add(1, std::memory_order_relaxed);
    }

  private:
    std::atomic<int>& count_;
};

class StoppableNode final : public takt::Node
{
  public:
        StoppableNode(std::atomic<bool>& worker_started,
                                    std::atomic<bool>& observed_stop)
                : takt::Node("stoppable-node"), worker_started_(worker_started),
                    observed_stop_(observed_stop)
    {
    }

  protected:
    struct WorkerTask final : public takt::Node::Task
    {
    };

    std::unique_ptr<takt::Node::Task> build_task(uint64_t sequence_id) override
    {
        if (sequence_id >= worker_count())
        {
            return nullptr;
        }
        return std::make_unique<WorkerTask>();
    }

    void process_task(size_t, takt::Node::Task&) override
    {
        worker_started_.store(true, std::memory_order_relaxed);
        while (!stop_requested())
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        observed_stop_.store(true, std::memory_order_relaxed);
    }

  private:
    std::atomic<bool>& worker_started_;
    std::atomic<bool>& observed_stop_;
};
} // namespace

TEST(NodeTests, SingleWorkerNodePipeDriven)
{
    takt::Pipe<int> serial_in("serial-in", 8, 0);
    takt::Pipe<int> serial_out("serial-out", 8, 0);
    SingleWorkerDoubleAgent serial_agent(serial_in, serial_out);
    serial_agent.start();
    for (int v = 1; v <= 3; ++v)
    {
        auto wt = serial_in.acquire_write(true);
        wt.value() = v;
        wt.publish();
    }
    serial_in.close();
    serial_agent.join();

    std::vector<int> serial_result;
    while (true)
    {
        try
        {
            auto rt = serial_out.acquire_read();
            serial_result.push_back(rt.value());
        }
        catch (const takt::PipeClosedAndEmptySignal&)
        {
            break;
        }
    }
    ASSERT_EQ(serial_result.size(), 3U);
    EXPECT_EQ(serial_result[0], 2);
    EXPECT_EQ(serial_result[1], 4);
    EXPECT_EQ(serial_result[2], 6);
}

TEST(NodeTests, MultiWorkerNodePipeDriven)
{
    std::atomic<int> parallel_sum{0};
    takt::Pipe<int> parallel_in("parallel-in", 128, 0);
    MultiWorkerSumAgent parallel_agent(parallel_in, parallel_sum, 4);
    parallel_agent.start();
    for (int i = 0; i < 100; ++i)
    {
        auto wt = parallel_in.acquire_write(true);
        wt.value() = 1;
        wt.publish();
    }
    parallel_in.close();
    parallel_agent.join();
    EXPECT_EQ(parallel_sum.load(), 100);
}

TEST(NodeTests, SingleWorkerExceptionContext)
{
    FaultySingleWorkerAgent faulty;
    faulty.set_stage_name("fault-stage");
    faulty.add_input_port_name("input-A");
    faulty.add_output_port_name("output-B");
    faulty.start();
    faulty.join();
    ASSERT_TRUE(faulty.has_capture());

    const auto& ctx = faulty.captured();
    EXPECT_EQ(ctx.node_name, "faulty-single-worker");
    EXPECT_EQ(ctx.stage_name, "fault-stage");
    ASSERT_EQ(ctx.input_port_names.size(), 1U);
    ASSERT_EQ(ctx.output_port_names.size(), 1U);
    EXPECT_EQ(ctx.input_port_names[0], "input-A");
    EXPECT_EQ(ctx.output_port_names[0], "output-B");
    EXPECT_EQ(ctx.message, "boom");
    EXPECT_FALSE(ctx.has_worker_index);
}

TEST(NodeTests, MultiWorkerExceptionContext)
{
    FaultyMultiWorkerAgent faulty_parallel;
    faulty_parallel.set_stage_name("parallel-fault-stage");
    faulty_parallel.add_input_port_name("parallel-in");
    faulty_parallel.add_output_port_name("parallel-out");
    faulty_parallel.start();
    faulty_parallel.join();
    ASSERT_TRUE(faulty_parallel.has_capture());

    const auto& pctx = faulty_parallel.captured();
    EXPECT_EQ(pctx.node_name, "faulty-multi-worker");
    EXPECT_EQ(pctx.stage_name, "parallel-fault-stage");
    ASSERT_EQ(pctx.input_port_names.size(), 1U);
    ASSERT_EQ(pctx.output_port_names.size(), 1U);
    EXPECT_EQ(pctx.input_port_names[0], "parallel-in");
    EXPECT_EQ(pctx.output_port_names[0], "parallel-out");
    EXPECT_EQ(pctx.message, "kaboom");
    EXPECT_TRUE(pctx.has_worker_index);
    EXPECT_LT(pctx.worker_index, 2U);
}

TEST(NodeTests, FailureLogsIncludeObservabilityContext)
{
    ScopedLoggerReset logger_reset;
    auto logger = std::make_shared<CapturingLogger>();
    takt::set_logger(logger);

    FaultySingleWorkerAgent faulty;
    faulty.set_pipeline_name("obs-pipeline");
    faulty.set_stage_name("fault-stage");
    faulty.add_input_port_name("in-a");
    faulty.add_output_port_name("out-b");

    faulty.start();
    faulty.join();

    EXPECT_TRUE(logger->has_error_substring("pipeline='obs-pipeline'"));
    EXPECT_TRUE(logger->has_error_substring("node='faulty-single-worker'"));
    EXPECT_TRUE(logger->has_error_substring("edge_in='in-a'"));
    EXPECT_TRUE(logger->has_error_substring("edge_out='out-b'"));
    EXPECT_TRUE(logger->has_error_substring("stop_reason='exception'"));
    EXPECT_TRUE(logger->has_error_substring("exception_summary='boom'"));
}

TEST(NodeTests, NodeBaseStoresMultiplePipeNames)
{
    struct ProbeAgent final : takt::Node
    {
        ProbeAgent() : takt::Node("probe") {}

        struct WorkerTask final : public takt::Node::Task
        {
        };

        std::unique_ptr<takt::Node::Task> build_task(uint64_t sequence_id) override
        {
            if (sequence_id >= worker_count())
            {
                return nullptr;
            }
            return std::make_unique<WorkerTask>();
        }

        void process_task(size_t, takt::Node::Task&) override {}
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

TEST(NodeTests, MultiStageNodesReconstructOrderedOutput)
{
    const std::string slogan = "We will rock you!";
    const std::vector<size_t> shuffled_order = {2, 3,  8,  4, 0,  11, 5,  6, 7,
                                                1, 12, 13, 9, 10, 15, 16, 14};

    takt::Pipe<IndexedChar> in("ordered-in", 32, IndexedChar{});
    takt::Pipe<char> out("ordered-out", 32, '\0');
    OrderedCharStore store;

    IngressReorderAgent stage1(in, store);
    OrderedEmitAgent stage2(store, out);

    stage1.start();
    stage2.start();

    for (size_t idx : shuffled_order)
    {
        auto w = in.acquire_write(true);
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

TEST(NodeTests, SubgraphNodeStartsAndJoinsInnerRuntime)
{
    std::atomic<int> run_count{0};
    std::vector<std::shared_ptr<takt::NodeBase>> inner_nodes;
    inner_nodes.push_back(std::make_shared<CountingNode>(run_count));
    inner_nodes.push_back(std::make_shared<CountingNode>(run_count));

    auto inner_runtime =
        std::make_shared<takt::PipelineRuntime>(std::move(inner_nodes));
    takt::SubgraphNode subgraph("sg", inner_runtime);

    subgraph.start();
    subgraph.join();

    EXPECT_EQ(run_count.load(std::memory_order_relaxed), 2);
}

TEST(NodeTests, SubgraphNodeRequestStopPropagatesToInnerRuntime)
{
    std::atomic<bool> worker_started{false};
    std::atomic<bool> observed_stop{false};
    std::vector<std::shared_ptr<takt::NodeBase>> inner_nodes;
    inner_nodes.push_back(
        std::make_shared<StoppableNode>(worker_started, observed_stop));

    auto inner_runtime =
        std::make_shared<takt::PipelineRuntime>(std::move(inner_nodes));
    takt::SubgraphNode subgraph("sg-stop", inner_runtime);

    subgraph.start();
    const auto deadline = std::chrono::steady_clock::now() +
                          std::chrono::milliseconds(200);
    while (!worker_started.load(std::memory_order_relaxed) &&
           std::chrono::steady_clock::now() < deadline)
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    subgraph.request_stop();
    subgraph.join();

    EXPECT_TRUE(worker_started.load(std::memory_order_relaxed));
    EXPECT_TRUE(observed_stop.load(std::memory_order_relaxed));
}
