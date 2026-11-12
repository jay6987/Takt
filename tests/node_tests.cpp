// Tests node worker admission, lifecycle hooks and exception propagation.
#include "pipe_test_support.h"
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
    // Binds pipes to a single persistent doubling worker.
    SingleWorkerDoubleAgent(takt::Pipe<int>& in,
                            takt::Pipe<int>& out)
        : takt::Node("single-worker-double"),
          in_(in),
          out_(out)
    {
    }

  protected:
    struct WorkerTask final : public takt::Node::Task
    {
    };

    // Admits one persistent task per configured worker.
    std::unique_ptr<takt::Node::Task> build_task(uint64_t sequence_id) override
    {
        if (sequence_id >= worker_count())
        {
            return nullptr;
        }
        return std::make_unique<WorkerTask>();
    }

    // Doubles frames until normal input completion closes output.
    void process_task(size_t,
                      takt::Node::Task&) override
    {
        while (true)
        {
            try
            {
                auto input = in_.acquire_read();
                auto output = input.is_shot_end()
                                  ? out_.acquire_shot_end_write(1)
                                  : out_.acquire_write();
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
    // Binds shared input and an atomic sum to the configured worker count.
    MultiWorkerSumAgent(takt::Pipe<int>& in,
                        std::atomic<int>& sum,
                        size_t workers)
        : takt::Node("multi-worker-sum",
                     workers),
          in_(in),
          sum_(sum)
    {
    }

  protected:
    struct WorkerTask final : public takt::Node::Task
    {
    };

    // Admits one persistent task for each permanent sum worker.
    std::unique_ptr<takt::Node::Task> build_task(uint64_t sequence_id) override
    {
        if (sequence_id >= worker_count())
        {
            return nullptr;
        }
        return std::make_unique<WorkerTask>();
    }

    // Atomically accumulates input frames until the pipe is drained.
    void process_task(size_t,
                      takt::Node::Task&) override
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
    // Creates a single-worker node with deterministic processing failure.
    FaultySingleWorkerAgent() : takt::Node("faulty-single-worker") {}

    // Returns failure context captured after worker execution has joined.
    const takt::NodeExceptionContext& captured() const
    {
        return captured_;
    }

    // Reports whether the exception hook captured a worker failure.
    bool has_capture() const
    {
        return has_capture_;
    }

  protected:
    struct WorkerTask final : public takt::Node::Task
    {
    };

    // Admits one persistent task that will trigger the injected failure.
    std::unique_ptr<takt::Node::Task> build_task(uint64_t sequence_id) override
    {
        if (sequence_id >= worker_count())
        {
            return nullptr;
        }
        return std::make_unique<WorkerTask>();
    }

    // Injects the known single-worker processing exception.
    void process_task(size_t,
                      takt::Node::Task&) override
    {
        throw std::runtime_error("boom");
    }

    // Saves exception context and preserves the default structured logger.
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
    // Creates two workers with deterministic processing failures.
    FaultyMultiWorkerAgent()
        : takt::Node("faulty-multi-worker",
                     2)
    {
    }

    // Returns captured context after all failing workers have joined.
    const takt::NodeExceptionContext& captured() const
    {
        return captured_;
    }

    // Reports whether any worker's exception hook captured a failure.
    bool has_capture() const
    {
        return has_capture_;
    }

  protected:
    struct WorkerTask final : public takt::Node::Task
    {
    };

    // Admits one persistent task for each failing worker.
    std::unique_ptr<takt::Node::Task> build_task(uint64_t sequence_id) override
    {
        if (sequence_id >= worker_count())
        {
            return nullptr;
        }
        return std::make_unique<WorkerTask>();
    }

    // Injects the known multiworker processing exception.
    void process_task(size_t,
                      takt::Node::Task&) override
    {
        throw std::runtime_error("kaboom");
    }

    // Serializes captured failure context and preserves standard logging.
    void on_exception(const takt::NodeExceptionContext& context) override
    {
        {
            std::lock_guard<std::mutex> lk(capture_mutex_);
            captured_ = context;
            has_capture_ = true;
        }
        takt::Node::on_exception(context);
    }

  private:
    std::mutex capture_mutex_;
    takt::NodeExceptionContext captured_;
    bool has_capture_ = false;
};

class CapturingLogger final : public takt::ILogger
{
  public:
    // Discards undecorated narrow output during failure assertions.
    void display(const std::string&) override {}
    // Discards undecorated wide output during failure assertions.
    void display(const std::wstring&) override {}
    // Discards narrow informational output.
    void info(const std::string&) override {}
    // Discards wide informational output.
    void info(const std::wstring&) override {}
    // Discards narrow warning output.
    void warn(const std::string&) override {}
    // Discards wide warning output.
    void warn(const std::wstring&) override {}
    // Stores narrow errors under synchronization for later assertions.
    void error(const std::string& message) override
    {
        std::lock_guard<std::mutex> lk(mu_);
        error_messages_.push_back(message);
    }
    // Discards wide errors unused by the structured failure logger.
    void error(const std::wstring&) override {}
    // Discards narrow debug output.
    void debug(const std::string&) override {}
    // Discards wide debug output.
    void debug(const std::wstring&) override {}
    // Keeps debug disabled for deterministic failure assertions.
    bool debug_enabled() const override
    {
        return false;
    }

    // Searches captured error messages while holding their synchronization
    // lock.
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
    // Restores default logging even when a test exits through a failed
    // assertion.
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
    // Stores an indexed character and wakes ordered consumers.
    void put(size_t idx,
             char c)
    {
        std::lock_guard<std::mutex> lk(mu_);
        store_[idx] = c;
        cv_.notify_all();
    }

    // Marks ingress completion and wakes consumers waiting for missing indices.
    void mark_done()
    {
        std::lock_guard<std::mutex> lk(mu_);
        done_ = true;
        cv_.notify_all();
    }

    // Waits for the requested character or confirmed completion without it.
    bool wait_pop(size_t idx,
                  char& out)
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
    // Binds unordered indexed input to the synchronized ordered store.
    IngressReorderAgent(takt::Pipe<IndexedChar>& in,
                        OrderedCharStore& store)
        : takt::Node("ingress-reorder"),
          in_(in),
          store_(store)
    {
    }

  protected:
    struct WorkerTask final : public takt::Node::Task
    {
    };

    // Admits one persistent task that drains indexed input.
    std::unique_ptr<takt::Node::Task> build_task(uint64_t sequence_id) override
    {
        if (sequence_id >= worker_count())
        {
            return nullptr;
        }
        return std::make_unique<WorkerTask>();
    }

    // Stores each indexed character and signals completion after input drains.
    void process_task(size_t,
                      takt::Node::Task&) override
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
    // Binds the ordered character store to a frame output pipe.
    OrderedEmitAgent(OrderedCharStore& store,
                     takt::Pipe<char>& out)
        : takt::Node("ordered-emit"),
          store_(store),
          out_(out)
    {
    }

  protected:
    struct WorkerTask final : public takt::Node::Task
    {
    };

    // Admits one persistent task responsible for ordered output.
    std::unique_ptr<takt::Node::Task> build_task(uint64_t sequence_id) override
    {
        if (sequence_id >= worker_count())
        {
            return nullptr;
        }
        return std::make_unique<WorkerTask>();
    }

    // Publishes consecutive characters and closes output when the store drains.
    void process_task(size_t,
                      takt::Node::Task&) override
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
            auto w = out_.acquire_shot_end_write(1);
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
    // Binds an execution counter used to verify nested startup.
    explicit CountingNode(std::atomic<int>& count)
        : takt::Node("counting-node"),
          count_(count)
    {
    }

  protected:
    struct WorkerTask final : public takt::Node::Task
    {
    };

    // Admits one counter-update task per configured worker.
    std::unique_ptr<takt::Node::Task> build_task(uint64_t sequence_id) override
    {
        if (sequence_id >= worker_count())
        {
            return nullptr;
        }
        return std::make_unique<WorkerTask>();
    }

    // Counts successful execution of a nested worker task.
    void process_task(size_t,
                      takt::Node::Task&) override
    {
        count_.fetch_add(1, std::memory_order_relaxed);
    }

  private:
    std::atomic<int>& count_;
};

class StoppableNode final : public takt::Node
{
  public:
    // Binds startup and cancellation observation signals for lifecycle tests.
    StoppableNode(std::promise<void>& started_promise,
                  std::atomic<bool>& observed_stop)
        : takt::Node("stoppable-node"),
          started_promise_(started_promise),
          observed_stop_(observed_stop)
    {
    }

  protected:
    struct WorkerTask final : public takt::Node::Task
    {
    };

    // Admits one persistent task that waits for cooperative cancellation.
    std::unique_ptr<takt::Node::Task> build_task(uint64_t sequence_id) override
    {
        if (sequence_id >= worker_count())
        {
            return nullptr;
        }
        return std::make_unique<WorkerTask>();
    }

    // Signals startup, waits for stop and records observed cancellation.
    void process_task(size_t,
                      takt::Node::Task&) override
    {
        started_promise_.set_value();
        while (!stop_requested())
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        observed_stop_.store(true, std::memory_order_relaxed);
    }

  private:
    std::promise<void>& started_promise_;
    std::atomic<bool>& observed_stop_;
};

class LifecycleProbe final : public takt::Node
{
  public:
    // Inject one failure into a framework-controlled execution phase.
    LifecycleProbe(int phase,
                   bool unknown = false,
                   bool throwing_observer = false)
        : Node("lifecycle"),
          phase_(phase),
          unknown_(unknown),
          throwing_observer_(throwing_observer)
    {
    }
    // Join while task destructors and hooks can still access probe state.
    ~LifecycleProbe() override
    {
        request_stop();
        join();
    }
    std::atomic<size_t> live{0}, processed{0}, barriers{0}, cleanup{0};
    std::atomic<bool> reported_unknown{false};

  private:
    struct ProbeTask final : Task
    {
        LifecycleProbe& owner;
        // Keep track of admitted payloads through successful or failed work.
        explicit ProbeTask(LifecycleProbe& probe) : owner(probe)
        {
            ++owner.live;
        }
        // Task destruction must precede ingress completion notifications.
        ~ProbeTask() override
        {
            --owner.live;
        }
    };
    // Throw the original cause without disguising nonstandard exceptions.
    void fail_at(int phase)
    {
        if (phase_ != phase)
            return;
        if (unknown_)
            throw 42;
        throw std::domain_error("original lifecycle failure");
    }
    // Emit a barrier payload, or a terminal stop payload for stop coverage.
    std::unique_ptr<Task> build_task(uint64_t sequence) override
    {
        fail_at(0);
        if (sequence != 0)
            return nullptr;
        auto task = std::make_unique<ProbeTask>(*this);
        task->barrier = true;
        if (phase_ == 6)
            task->kind = TaskKind::Stop;
        return task;
    }
    // Count work and exercise failure before the task completion hook.
    void process_task(size_t,
                      Task&) override
    {
        ++processed;
        fail_at(1);
    }
    // Exercise cleanup when a post-processing worker hook throws.
    void on_task_done(size_t,
                      const Task&) override
    {
        fail_at(2);
    }
    // Observe payload destruction before group publication on ingress.
    void on_barrier_complete(uint64_t) override
    {
        EXPECT_EQ(live.load(), 0U);
        fail_at(3);
        ++barriers;
    }
    // Check drain on successful EOF, failed processing and cancellation.
    void on_ingress_manager_done() override
    {
        EXPECT_EQ(live.load(), 0U);
        ++cleanup;
        fail_at(4);
    }
    // Exercise retained failures after a permanent worker terminates.
    void on_worker_done(size_t) override
    {
        fail_at(5);
    }
    // Observe nonstandard causes and optionally throw a secondary error.
    void on_exception(const takt::NodeExceptionContext& context) override
    {
        reported_unknown = context.unknown_exception;
        if (throwing_observer_)
            throw std::runtime_error("secondary observer failure");
        Node::on_exception(context);
    }
    int phase_;
    bool unknown_;
    bool throwing_observer_;
};
} // namespace

// Every framework hook retains its original error and drains task payloads.
TEST(NodeTests,
     HookFailuresDrainAndPreserveOriginalCause)
{
    for (int phase = 0; phase <= 5; ++phase)
    {
        LifecycleProbe node(phase, false, phase == 2);
        node.start();
        EXPECT_NO_THROW(node.join());
        ASSERT_NE(node.failure(), nullptr);
        try
        {
            std::rethrow_exception(node.failure());
        }
        catch (const std::domain_error& error)
        {
            EXPECT_STREQ(error.what(), "original lifecycle failure");
        }
        EXPECT_EQ(node.live.load(), 0U);
        EXPECT_EQ(node.cleanup.load(), 1U);
        if (phase <= 3)
            EXPECT_EQ(node.barriers.load(), 0U);
    }
}

// Nonstandard failures from ingress and workers use structured diagnostics.
TEST(NodeTests,
     NonstandardFailuresRemainObservable)
{
    for (int phase : {0, 1})
    {
        LifecycleProbe node(phase, true);
        node.start();
        node.join();
        ASSERT_NE(node.failure(), nullptr);
        EXPECT_TRUE(node.reported_unknown.load());
        EXPECT_EQ(node.live.load(), 0U);
        try
        {
            std::rethrow_exception(node.failure());
        }
        catch (int cause)
        {
            EXPECT_EQ(cause, 42);
        }
    }
}

// A stop payload closes admission rather than leaving ingress without workers.
TEST(NodeTests,
     StopTaskTerminatesAdmissionWithoutProcessingOrBarrierPublication)
{
    LifecycleProbe node(6);
    node.start();
    node.join();
    EXPECT_EQ(node.failure(), nullptr);
    EXPECT_EQ(node.processed.load(), 0U);
    EXPECT_EQ(node.barriers.load(), 0U);
    EXPECT_EQ(node.cleanup.load(), 1U);
    EXPECT_EQ(node.live.load(), 0U);
}

// Verifies a single processing worker consumes and transforms pipe frames.
TEST(NodeTests,
     SingleWorkerNodePipeDriven)
{
    takt::Pipe<int> serial_in("serial-in");
    takt::Pipe<int> serial_out("serial-out");
    configure_pipe(serial_in, 8, 0);
    configure_pipe(serial_out, 8, 0);
    serial_in.initialize();
    serial_out.initialize();
    SingleWorkerDoubleAgent serial_agent(serial_in, serial_out);
    serial_agent.start();
    for (int v = 1; v <= 3; ++v)
    {
        auto wt = serial_in.acquire_shot_end_write(1);
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

// Verifies multiple permanent workers consume every frame exactly once.
TEST(NodeTests,
     MultiWorkerNodePipeDriven)
{
    std::atomic<int> parallel_sum{0};
    takt::Pipe<int> parallel_in("parallel-in");
    configure_pipe(parallel_in, 128, 0);
    parallel_in.initialize();
    MultiWorkerSumAgent parallel_agent(parallel_in, parallel_sum, 4);
    parallel_agent.start();
    for (int i = 0; i < 100; ++i)
    {
        auto wt = parallel_in.acquire_shot_end_write(1);
        wt.value() = 1;
        wt.publish();
    }
    parallel_in.close();
    parallel_agent.join();
    EXPECT_EQ(parallel_sum.load(), 100);
}

// Verifies failures carry the single worker's structured diagnostic context.
TEST(NodeTests,
     SingleWorkerExceptionContext)
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

// Verifies multiworker failure reports identify the failing worker.
TEST(NodeTests,
     MultiWorkerExceptionContext)
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

// Verifies logged failures include pipeline, stage and port labels.
TEST(NodeTests,
     FailureLogsIncludeObservabilityContext)
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

// Verifies node context retains all declared input and output names.
TEST(NodeTests,
     NodeBaseStoresMultiplePipeNames)
{
    struct ProbeAgent final : takt::Node
    {
        ProbeAgent() : takt::Node("probe") {}

        struct WorkerTask final : public takt::Node::Task
        {
        };

        std::unique_ptr<takt::Node::Task>
        build_task(uint64_t sequence_id) override
        {
            if (sequence_id >= worker_count())
            {
                return nullptr;
            }
            return std::make_unique<WorkerTask>();
        }

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
