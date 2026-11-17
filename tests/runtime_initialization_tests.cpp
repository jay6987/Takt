// Verifies runtime preparation phases, identity deduplication, and failures.
#include "runtime_initialization_tests_test_support.h"

// Verifies explicit initialization is required even for an empty runtime.
TEST(RuntimeInitialization,
     ExplicitPhaseAndEmptyRuntime)
{
    auto probe = std::make_shared<Probe>();
    takt::PipelineRuntime runtime({probe});
    EXPECT_THROW(runtime.start(), std::logic_error);
    EXPECT_EQ(probe->starts, 0);
    runtime.initialize();
    EXPECT_TRUE(runtime.initialized());
    EXPECT_THROW(runtime.initialize(), std::logic_error);
    runtime.start();
    runtime.start();
    EXPECT_EQ(probe->starts, 1);
}

// Verifies early cancellation prevents registered pipe initialization.
TEST(RuntimeInitialization,
     StopBeforeInitializationCancelsRegisteredPipes)
{
    auto pipe = std::make_shared<takt::Pipe<int>>("cancelled");
    pipe->set_producer(
        {"source", takt::concurrency(1), takt::frames_per_write(1)}, 0);
    pipe->set_consumer({"sink", takt::concurrency(1), takt::frames_per_read(1),
                        takt::overlap_frames(0)});
    auto probe = std::make_shared<Probe>();
    takt::PipelineRuntime runtime({probe}, {pipe});
    runtime.request_stop();
    EXPECT_TRUE(pipe->closed());
    EXPECT_THROW(runtime.initialize(), std::logic_error);
    EXPECT_THROW(runtime.start(), std::logic_error);
    EXPECT_EQ(probe->starts, 0);
}

// Verifies preparation deduplicates pipe objects rather than diagnostic names.
TEST(RuntimeInitialization,
     WholeTreeDeduplicatesIdentityNotNames)
{
    auto shared = std::make_shared<CountingPipe>();
    auto different = std::make_shared<CountingPipe>();
    auto first = std::make_shared<Probe>();
    auto second = std::make_shared<Probe>();
    auto first_runtime = std::make_shared<takt::PipelineRuntime>(
        std::vector<std::shared_ptr<takt::NodeBase>>{first},
        std::vector<std::shared_ptr<takt::PipeBase>>{shared, shared});
    auto second_runtime = std::make_shared<takt::PipelineRuntime>(
        std::vector<std::shared_ptr<takt::NodeBase>>{second},
        std::vector<std::shared_ptr<takt::PipeBase>>{shared, different});
    auto outer = std::make_shared<Probe>();
    outer->children = {first_runtime, second_runtime};
    takt::PipelineRuntime root({outer}, {shared});
    root.initialize();
    EXPECT_TRUE(first_runtime->initialized());
    EXPECT_TRUE(second_runtime->initialized());
    EXPECT_EQ(shared->initializations, 1);
    EXPECT_EQ(different->initializations, 1);
    EXPECT_EQ(outer->starts + first->starts + second->starts, 0);
}

// Verifies full-tree validation precedes allocation of any frame storage.
TEST(RuntimeInitialization,
     ConfigurationFailureAllocatesNothing)
{
    auto good = std::make_shared<CountingPipe>();
    auto bad = std::make_shared<CountingPipe>();
    bad->invalid = true;
    auto probe = std::make_shared<Probe>();
    takt::PipelineRuntime runtime({probe}, {good, bad});
    EXPECT_THROW(runtime.initialize(), std::logic_error);
    EXPECT_EQ(good->initializations, 0);
    EXPECT_EQ(bad->initializations, 0);
    EXPECT_THROW(runtime.start(), std::logic_error);
    EXPECT_EQ(probe->starts, 0);
    EXPECT_NO_THROW(runtime.request_stop());
    EXPECT_NO_THROW(runtime.join());
}

// Verifies failed allocation leaves a terminal runtime with its original cause.
TEST(RuntimeInitialization,
     AllocationFailureIsTerminalAndRetainsCause)
{
    auto good = std::make_shared<CountingPipe>();
    auto bad = std::make_shared<CountingPipe>();
    bad->allocation_failure = true;
    auto probe = std::make_shared<Probe>();
    takt::PipelineRuntime runtime({probe}, {good, bad});
    EXPECT_THROW(runtime.initialize(), std::runtime_error);
    EXPECT_TRUE(good->initialized());
    EXPECT_FALSE(bad->initialized());
    try
    {
        runtime.start();
        FAIL() << "failed runtime must not start";
    }
    catch (const std::runtime_error& error)
    {
        EXPECT_STREQ(error.what(), "allocation failure");
    }
    EXPECT_EQ(probe->starts, 0);
}

// Verifies failed initialization does not leak lifecycle pipe ownership.
TEST(RuntimeInitialization,
     FailedRuntimeDoesNotRetainPipesBeyondItsOwner)
{
    std::weak_ptr<CountingPipe> weak;
    {
        auto pipe = std::make_shared<CountingPipe>();
        weak = pipe;
        pipe->allocation_failure = true;
        takt::PipelineRuntime runtime({}, {pipe});
        EXPECT_THROW(runtime.initialize(), std::runtime_error);
        pipe.reset();
        EXPECT_FALSE(weak.expired());
    }
    EXPECT_TRUE(weak.expired());
}
