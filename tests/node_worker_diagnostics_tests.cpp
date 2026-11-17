// Verifies worker-driven processing and structured exception diagnostics.
#include "node_tests_test_support.h"

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
