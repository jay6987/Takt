#include "takt/takt.h"

#include <atomic>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

namespace
{
class ProbeNode final : public takt::Node
{
  public:
    explicit ProbeNode(std::atomic<int>& run_count)
        : takt::Node("probe"), run_count_(run_count)
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
        run_count_.fetch_add(1, std::memory_order_relaxed);
    }

  private:
    std::atomic<int>& run_count_;
};

class DoubleNode final : public takt::Node
{
  public:
    DoubleNode(std::shared_ptr<takt::Pipe<int>> in,
               std::shared_ptr<takt::Pipe<int>> out)
        : takt::Node("double-node"), in_(std::move(in)), out_(std::move(out))
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
                auto r = in_->acquire_read();
                auto w = out_->acquire_write(r.is_segment_end());
                w.value() = r.value() * 2;
                w.publish();
            }
            catch (const takt::PipeClosedAndEmptySignal&)
            {
                out_->close();
                break;
            }
        }
    }

  private:
    std::shared_ptr<takt::Pipe<int>> in_;
    std::shared_ptr<takt::Pipe<int>> out_;
};

class DrainStringNode final : public takt::Node
{
  public:
    explicit DrainStringNode(std::shared_ptr<takt::Pipe<std::string>> in)
        : takt::Node("drain-string-node"), in_(std::move(in))
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
                auto r = in_->acquire_read();
                (void)r.value();
            }
            catch (const takt::PipeClosedAndEmptySignal&)
            {
                break;
            }
        }
    }

  private:
    std::shared_ptr<takt::Pipe<std::string>> in_;
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
} // namespace

TEST(PipelineBuilderTests, SupportsMultipleInputsAndOutputs)
{
    auto builder = takt::PipelineBuilder::declare("graph-multi");
    builder.add_node("source");
    builder.add_node("transform");
    builder.add_node("sink");

    builder.add_output_port("source", "out0");
    builder.add_output_port("source", "out1");
    builder.add_input_port("transform", "in0");
    builder.add_input_port("transform", "in1");
    builder.add_output_port("transform", "out");
    builder.add_input_port("sink", "left");
    builder.add_input_port("sink", "right");

    builder.connect("source", "out0", "transform", "in0");
    builder.connect("source", "out1", "transform", "in1");
    builder.connect("transform", "out", "sink", "left");
    builder.connect("transform", "out", "sink", "right");

    EXPECT_EQ(builder.nodes().size(), 3U);
    EXPECT_EQ(builder.edges().size(), 4U);
    EXPECT_NO_THROW(builder.validate());
}

TEST(PipelineBuilderTests, AllowsCyclesInGraphModel)
{
    auto builder = takt::PipelineBuilder::declare("graph-cycle");
    builder.add_node("a");
    builder.add_node("b");
    builder.add_output_port("a", "out");
    builder.add_input_port("b", "in");
    builder.add_output_port("b", "out");
    builder.add_input_port("a", "in");

    builder.connect("a", "out", "b", "in");
    builder.connect("b", "out", "a", "in");

    EXPECT_EQ(builder.edges().size(), 2U);
    EXPECT_NO_THROW(builder.validate());
}

TEST(PipelineBuilderTests, AllowsDuplicateSameEndpointEdges)
{
    auto builder = takt::PipelineBuilder::declare("graph-duplicate-edges");
    builder.add_node("source");
    builder.add_node("sink");
    builder.add_output_port("source", "out");
    builder.add_input_port("sink", "in");

    builder.connect("source", "out", "sink", "in", {}, "e0");
    builder.connect("source", "out", "sink", "in", {}, "e1");

    EXPECT_EQ(builder.edges().size(), 2U);
    EXPECT_NO_THROW(builder.validate());
}

TEST(PipelineBuilderTests, BuildRuntimeInjectsPortMetadataToBoundNodes)
{
    auto builder = takt::PipelineBuilder::declare("graph-runtime-meta");
    builder.add_node("source");
    builder.add_node("transform");
    builder.add_output_port("source", "frames");
    builder.add_output_port("source", "masks");
    builder.add_input_port("transform", "frames");
    builder.add_input_port("transform", "masks");
    builder.connect("source", "frames", "transform", "frames");
    builder.connect("source", "masks", "transform", "masks");

    std::atomic<int> c0{0};
    std::atomic<int> c1{0};
    auto source = std::make_shared<ProbeNode>(c0);
    auto transform = std::make_shared<ProbeNode>(c1);

    builder.bind_node("source", source);
    builder.bind_node("transform", transform);

    auto runtime = builder.build_runtime();
    ASSERT_NE(runtime, nullptr);

    ASSERT_EQ(source->output_port_names().size(), 2U);
    EXPECT_EQ(source->output_port_names()[0], "frames");
    EXPECT_EQ(source->output_port_names()[1], "masks");
    ASSERT_TRUE(source->input_port_names().empty());

    ASSERT_EQ(transform->input_port_names().size(), 2U);
    EXPECT_EQ(transform->input_port_names()[0], "frames");
    EXPECT_EQ(transform->input_port_names()[1], "masks");
    ASSERT_TRUE(transform->output_port_names().empty());
}

TEST(PipelineBuilderTests, BuildRuntimeInjectsDeclaredPortsWithoutChannels)
{
    auto builder = takt::PipelineBuilder::declare("graph-runtime-declared-only");
    builder.add_node("standalone");
    builder.add_input_port("standalone", "in");
    builder.add_output_port("standalone", "out");

    std::atomic<int> count{0};
    auto node = std::make_shared<ProbeNode>(count);
    builder.bind_node("standalone", node);

    auto runtime = builder.build_runtime();
    ASSERT_NE(runtime, nullptr);
    ASSERT_EQ(node->input_port_names().size(), 1U);
    ASSERT_EQ(node->output_port_names().size(), 1U);
    EXPECT_EQ(node->input_port_names()[0], "in");
    EXPECT_EQ(node->output_port_names()[0], "out");
}

TEST(PipelineBuilderTests, RuntimeStartsAndJoinsBoundNodes)
{
    auto builder = takt::PipelineBuilder::declare("graph-runtime-run");
    builder.add_node("a");
    builder.add_node("b");

    std::atomic<int> run_count{0};
    builder.bind_node("a", std::make_shared<ProbeNode>(run_count));
    builder.bind_node("b", std::make_shared<ProbeNode>(run_count));

    auto runtime = builder.build_runtime();
    runtime->start();
    runtime->join();

    EXPECT_EQ(run_count.load(std::memory_order_relaxed), 2);
}

TEST(PipelineBuilderTests, BindSubgraphBuildsCompositeNode)
{
    auto inner = takt::PipelineBuilder::declare("graph-inner");
    inner.add_node("inner-a");
    inner.add_node("inner-b");

    std::atomic<int> inner_run_count{0};
    inner.bind_node("inner-a", std::make_shared<ProbeNode>(inner_run_count));
    inner.bind_node("inner-b", std::make_shared<ProbeNode>(inner_run_count));

    auto outer = takt::PipelineBuilder::declare("graph-outer");
    outer.add_node("subgraph-entry");
    outer.bind_subgraph("subgraph-entry", inner);

    auto runtime = outer.build_runtime();
    runtime->start();
    runtime->join();

    EXPECT_EQ(inner_run_count.load(std::memory_order_relaxed), 2);
}

TEST(PipelineBuilderTests, BindSubgraphWithPortMappingsStoresMappings)
{
    auto outer_in = std::make_shared<takt::Pipe<int>>("mapped-outer-in", 8, 0);
    auto outer_out = std::make_shared<takt::Pipe<int>>("mapped-outer-out", 8, 0);
    auto inner_in = std::make_shared<takt::Pipe<int>>("mapped-inner-in", 8, 0);
    auto inner_out = std::make_shared<takt::Pipe<int>>("mapped-inner-out", 8, 0);

    auto inner = takt::PipelineBuilder::declare("graph-inner-mapped");
    inner.add_node("entry");
    inner.add_node("exit");
    inner.add_input_port("entry", "in",
                         {{"pipe_endpoint", takt::make_pipe_endpoint<int>(inner_in)}});
    inner.add_output_port(
        "exit", "out", {{"pipe_endpoint", takt::make_pipe_endpoint<int>(inner_out)}});

    std::atomic<int> inner_run_count{0};
    inner.bind_node("entry", std::make_shared<ProbeNode>(inner_run_count));
    inner.bind_node("exit", std::make_shared<ProbeNode>(inner_run_count));

    auto outer = takt::PipelineBuilder::declare("graph-outer-mapped");
    outer.add_node("sg");
    outer.add_input_port("sg", "raw",
                         {{"pipe_endpoint", takt::make_pipe_endpoint<int>(outer_in)}});
    outer.add_output_port(
        "sg", "result", {{"pipe_endpoint", takt::make_pipe_endpoint<int>(outer_out)}});

    outer.bind_subgraph("sg", inner, {{"raw", {"entry", "in"}}},
                        {{"result", {"exit", "out"}}});

    auto bound = outer.find_bound_node("sg");
    auto subgraph_node = std::dynamic_pointer_cast<takt::SubgraphNode>(bound);
    ASSERT_NE(subgraph_node, nullptr);
    ASSERT_EQ(subgraph_node->input_port_mappings().size(), 1U);
    ASSERT_EQ(subgraph_node->output_port_mappings().size(), 1U);
    EXPECT_EQ(subgraph_node->input_port_mappings().at("raw").node_name, "entry");
    EXPECT_EQ(subgraph_node->input_port_mappings().at("raw").port_name, "in");
    EXPECT_EQ(subgraph_node->output_port_mappings().at("result").node_name, "exit");
    EXPECT_EQ(subgraph_node->output_port_mappings().at("result").port_name, "out");
}

TEST(PipelineBuilderTests, BindSubgraphRejectsMissingRequiredMapping)
{
    auto inner = takt::PipelineBuilder::declare("graph-inner-mapped-fail");
    inner.add_node("entry");
    inner.add_input_port("entry", "in");
    std::atomic<int> inner_run_count{0};
    inner.bind_node("entry", std::make_shared<ProbeNode>(inner_run_count));

    auto outer = takt::PipelineBuilder::declare("graph-outer-mapped-fail");
    outer.add_node("sg");
    outer.add_input_port("sg", "raw");

    EXPECT_THROW(outer.bind_subgraph("sg", inner, {}, {}), std::logic_error);
}

TEST(PipelineBuilderTests, SubgraphMappingBridgesRuntimeDataFlow)
{
    auto outer_in = std::make_shared<takt::Pipe<int>>("outer-in", 16, 0);
    auto outer_out = std::make_shared<takt::Pipe<int>>("outer-out", 16, 0);
    auto inner_in = std::make_shared<takt::Pipe<int>>("inner-in", 16, 0);
    auto inner_out = std::make_shared<takt::Pipe<int>>("inner-out", 16, 0);

    auto inner = takt::PipelineBuilder::declare("graph-inner-bridge");
    inner.add_node("worker");
    inner.add_input_port("worker", "in",
                         {{"pipe_endpoint", takt::make_pipe_endpoint<int>(inner_in)}});
    inner.add_output_port(
        "worker", "out", {{"pipe_endpoint", takt::make_pipe_endpoint<int>(inner_out)}});
    inner.bind_node("worker", std::make_shared<DoubleNode>(inner_in, inner_out));

    auto outer = takt::PipelineBuilder::declare("graph-outer-bridge");
    outer.add_node("sg");
    outer.add_input_port("sg", "raw",
                         {{"pipe_endpoint", takt::make_pipe_endpoint<int>(outer_in)}});
    outer.add_output_port(
        "sg", "result", {{"pipe_endpoint", takt::make_pipe_endpoint<int>(outer_out)}});

    outer.bind_subgraph("sg", inner, {{"raw", {"worker", "in"}}},
                        {{"result", {"worker", "out"}}});

    auto runtime = outer.build_runtime();
    runtime->start();

    for (int i = 1; i <= 4; ++i)
    {
        auto w = outer_in->acquire_write(true);
        w.value() = i;
        w.publish();
    }
    outer_in->close();

    runtime->join();

    std::vector<int> result;
    while (true)
    {
        try
        {
            auto r = outer_out->acquire_read();
            result.push_back(r.value());
        }
        catch (const takt::PipeClosedAndEmptySignal&)
        {
            break;
        }
    }

    ASSERT_EQ(result.size(), 4U);
    EXPECT_EQ(result[0], 2);
    EXPECT_EQ(result[1], 4);
    EXPECT_EQ(result[2], 6);
    EXPECT_EQ(result[3], 8);
}

TEST(PipelineBuilderTests, PipeEndpointWriteReportsTypeMismatch)
{
    auto pipe = std::make_shared<takt::Pipe<int>>("type-mismatch", 4, 0);
    auto endpoint = takt::make_pipe_endpoint<int>(pipe);

    takt::AnyPipePacket packet;
    packet.value = std::string("not-an-int");

    try
    {
        endpoint->write(packet);
        FAIL() << "expected PipeEndpointTypeMismatchError";
    }
    catch (const takt::PipeEndpointTypeMismatchError& e)
    {
        const std::string message = e.what();
        EXPECT_NE(message.find("type-mismatch"), std::string::npos);
        EXPECT_NE(message.find("expected='"), std::string::npos);
        EXPECT_NE(message.find("actual='"), std::string::npos);
    }
}

TEST(PipelineBuilderTests, SubgraphBridgeFailurePropagatesToRuntimeJoin)
{
    ScopedLoggerReset logger_reset;
    auto logger = std::make_shared<CapturingLogger>();
    takt::set_logger(logger);

    auto outer_in = std::make_shared<takt::Pipe<int>>("outer-in-mismatch", 8, 0);
    auto inner_in = std::make_shared<takt::Pipe<std::string>>("inner-in-mismatch", 8,
                                                              std::string());

    auto inner = takt::PipelineBuilder::declare("graph-inner-bridge-mismatch");
    inner.add_node("sink");
    inner.add_input_port(
        "sink", "in",
        {{"pipe_endpoint", takt::make_pipe_endpoint<std::string>(inner_in)}});
    inner.bind_node("sink", std::make_shared<DrainStringNode>(inner_in));

    auto outer = takt::PipelineBuilder::declare("graph-outer-bridge-mismatch");
    outer.add_node("sg");
    outer.add_input_port("sg", "raw",
                         {{"pipe_endpoint", takt::make_pipe_endpoint<int>(outer_in)}});

    outer.bind_subgraph("sg", inner, {{"raw", {"sink", "in"}}}, {});

    auto runtime = outer.build_runtime();
    runtime->start();

    {
        auto w = outer_in->acquire_write(true);
        w.value() = 42;
        w.publish();
    }
    outer_in->close();

    EXPECT_THROW(runtime->join(), std::runtime_error);
    EXPECT_TRUE(logger->has_error_substring("pipeline='graph-outer-bridge-mismatch'"));
    EXPECT_TRUE(logger->has_error_substring("node='sg'"));
    EXPECT_TRUE(logger->has_error_substring("edge='sg.raw->sink.in'"));
    EXPECT_TRUE(logger->has_error_substring("stop_reason='bridge_exception'"));
    EXPECT_TRUE(logger->has_error_substring("exception_summary='"));
}

TEST(PipelineBuilderTests, SubgraphLifecycleRequestStopJoinNoDeadlock)
{
    auto outer_in = std::make_shared<takt::Pipe<int>>("outer-in-lifecycle", 64, 0);
    auto outer_out = std::make_shared<takt::Pipe<int>>("outer-out-lifecycle", 64, 0);
    auto inner_in = std::make_shared<takt::Pipe<int>>("inner-in-lifecycle", 64, 0);
    auto inner_out = std::make_shared<takt::Pipe<int>>("inner-out-lifecycle", 64, 0);

    auto inner = takt::PipelineBuilder::declare("graph-inner-lifecycle");
    inner.add_node("worker");
    inner.add_input_port("worker", "in",
                         {{"pipe_endpoint", takt::make_pipe_endpoint<int>(inner_in)}});
    inner.add_output_port(
        "worker", "out", {{"pipe_endpoint", takt::make_pipe_endpoint<int>(inner_out)}});
    inner.bind_node("worker", std::make_shared<DoubleNode>(inner_in, inner_out));

    auto outer = takt::PipelineBuilder::declare("graph-outer-lifecycle");
    outer.add_node("sg");
    outer.add_input_port("sg", "raw",
                         {{"pipe_endpoint", takt::make_pipe_endpoint<int>(outer_in)}});
    outer.add_output_port(
        "sg", "result", {{"pipe_endpoint", takt::make_pipe_endpoint<int>(outer_out)}});

    outer.bind_subgraph("sg", inner, {{"raw", {"worker", "in"}}},
                        {{"result", {"worker", "out"}}});

    auto runtime = outer.build_runtime();
    runtime->start();

    std::atomic<bool> stop_writer{false};
    std::thread writer(
        [&]
        {
            int v = 1;
            while (!stop_writer.load(std::memory_order_relaxed))
            {
                try
                {
                    auto w = outer_in->acquire_write(true);
                    w.value() = v++;
                    w.publish();
                }
                catch (const takt::PipeClosedAndEmptySignal&)
                {
                    break;
                }
            }
        });

    runtime->request_stop();
    EXPECT_NO_THROW(runtime->join());

    stop_writer.store(true, std::memory_order_relaxed);
    if (writer.joinable())
    {
        writer.join();
    }
}