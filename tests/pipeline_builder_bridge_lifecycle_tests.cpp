// Verifies nested bridge failure propagation and deadlock-free cancellation.
#include "pipeline_builder_tests_test_support.h"

namespace
{
// Builds the nested bridge runtime used to exercise cancellation and joining.
std::shared_ptr<takt::PipelineRuntime>
make_lifecycle_runtime(const std::shared_ptr<takt::Pipe<int>>& outer_in,
                       const std::shared_ptr<takt::Pipe<int>>& outer_out,
                       const std::shared_ptr<takt::Pipe<int>>& inner_in,
                       const std::shared_ptr<takt::Pipe<int>>& inner_out)
{
    auto inner = takt::PipelineBuilder::create("graph-inner-lifecycle");
    inner.add_node("worker");
    inner.add_input_port(
        "worker", "in",
        {{"pipe_endpoint", takt::make_pipe_endpoint<int>(inner_in)}});
    inner.add_output_port(
        "worker", "out",
        {{"pipe_endpoint", takt::make_pipe_endpoint<int>(inner_out)}});
    inner.bind_node("worker",
                    std::make_shared<DoubleNode>(inner_in, inner_out));

    auto outer = takt::PipelineBuilder::create("graph-outer-lifecycle");
    outer.add_node("sg");
    outer.add_input_port(
        "sg", "raw",
        {{"pipe_endpoint", takt::make_pipe_endpoint<int>(outer_in)}});
    outer.add_output_port(
        "sg", "result",
        {{"pipe_endpoint", takt::make_pipe_endpoint<int>(outer_out)}});
    outer.bind_subgraph("sg", inner, {{"raw", {"worker", "in"}}},
                        {{"result", {"worker", "out"}}});
    return outer.build_runtime();
}

// Publishes sequential frames until cancellation stops the source pipe.
void write_until_closed(const std::shared_ptr<takt::Pipe<int>>& input,
                        const std::atomic<bool>& stop_writer)
{
    int value = 1;
    while (!stop_writer.load(std::memory_order_relaxed))
    {
        try
        {
            auto write = input->acquire_shot_end_write(1);
            write.value() = value++;
            write.publish();
        }
        catch (const takt::PipeClosedAndEmptySignal&)
        {
            break;
        }
    }
}
} // namespace

// Verifies boundary transfer failures propagate to the owning runtime.
TEST(PipelineBuilderTests,
     SubgraphBridgeFailurePropagatesToRuntimeJoin)
{
    ScopedLoggerReset logger_reset;
    auto logger = std::make_shared<CapturingLogger>();
    takt::set_logger(logger);

    auto outer_in = make_configured_pipe("outer-in-mismatch", 8, 0);
    auto inner_in = make_configured_pipe("inner-in-mismatch", 8, std::string());

    auto inner = takt::PipelineBuilder::create("graph-inner-bridge-mismatch");
    inner.add_node("sink");
    inner.add_input_port(
        "sink", "in",
        {{"pipe_endpoint", takt::make_pipe_endpoint<std::string>(inner_in)}});
    inner.bind_node("sink", std::make_shared<DrainStringNode>(inner_in));

    auto outer = takt::PipelineBuilder::create("graph-outer-bridge-mismatch");
    outer.add_node("sg");
    outer.add_input_port(
        "sg", "raw",
        {{"pipe_endpoint", takt::make_pipe_endpoint<int>(outer_in)}});

    outer.bind_subgraph("sg", inner, {{"raw", {"sink", "in"}}}, {});

    auto runtime = outer.build_runtime();
    ASSERT_NO_THROW(runtime->initialize());
    EXPECT_TRUE(outer_in->initialized());
    EXPECT_TRUE(inner_in->initialized());
    runtime->start();

    {
        auto w = outer_in->acquire_shot_end_write(1);
        w.value() = 42;
        w.publish();
    }
    outer_in->close();

    EXPECT_THROW(runtime->join(), std::runtime_error);
    EXPECT_TRUE(
        logger->has_error_substring("pipeline='graph-outer-bridge-mismatch'"));
    EXPECT_TRUE(logger->has_error_substring("node='sg'"));
    EXPECT_TRUE(logger->has_error_substring("edge='sg.raw->sink.in'"));
    EXPECT_TRUE(logger->has_error_substring("stop_reason='bridge_exception'"));
    EXPECT_TRUE(logger->has_error_substring("exception_summary='"));
}

// Verifies cancellation and joining release blocked subgraph workers.
TEST(PipelineBuilderTests,
     SubgraphLifecycleRequestStopJoinNoDeadlock)
{
    auto outer_in = make_configured_pipe("outer-in-lifecycle", 64, 0);
    auto outer_out = make_configured_pipe("outer-out-lifecycle", 64, 0);
    auto inner_in = make_configured_pipe("inner-in-lifecycle", 64, 0);
    auto inner_out = make_configured_pipe("inner-out-lifecycle", 64, 0);

    auto runtime =
        make_lifecycle_runtime(outer_in, outer_out, inner_in, inner_out);
    runtime->initialize();
    runtime->start();

    std::atomic<bool> stop_writer{false};
    std::thread writer([&] { write_until_closed(outer_in, stop_writer); });

    runtime->request_stop();
    EXPECT_NO_THROW(runtime->join());

    stop_writer.store(true, std::memory_order_relaxed);
    if (writer.joinable())
    {
        writer.join();
    }
}