// Verifies node lifecycle failures, cleanup hooks, and retained error causes.
#include "node_tests_test_support.h"

// Every framework hook retains its original error and drains task payloads.
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
