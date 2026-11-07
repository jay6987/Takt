#include "takt/takt.h"

#include <gtest/gtest.h>

#include <memory>
#include <stdexcept>
#include <vector>

namespace
{
class Probe : public takt::NodeBase
{
  public:
    Probe() : NodeBase("probe") {}
    void start() override
    {
        ++starts;
    }
    void join() override {}
    std::vector<std::shared_ptr<takt::PipelineRuntime>> child_runtimes() const override
    {
        return children;
    }
    int starts = 0;
    std::vector<std::shared_ptr<takt::PipelineRuntime>> children;
};

class FailingStart final : public Probe
{
  public:
    void start() override
    {
        throw std::runtime_error("startup failure");
    }
};

class CountingPipe final : public takt::PipeBase
{
  public:
    CountingPipe() : PipeBase("same-name")
    {
        set_producer_config({"producer", 1, 1});
        set_consumer({"consumer", 1, 1, 0});
    }
    bool closed() const noexcept override
    {
        return false;
    }
    bool faulted() const noexcept override
    {
        return false;
    }
    void close() override {}
    void validate_configuration() const override
    {
        validate_common();
        if (invalid)
            throw std::logic_error("invalid configuration");
    }
    void initialize() override
    {
        validate_configuration();
        ++initializations;
        if (allocation_failure)
            throw std::runtime_error("allocation failure");
        initialized_ = true;
    }
    bool invalid = false;
    bool allocation_failure = false;
    int initializations = 0;
};

TEST(RuntimeInitialization, ExplicitPhaseAndEmptyRuntime)
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

TEST(RuntimeInitialization, StopBeforeInitializationCancelsRegisteredPipes)
{
    auto pipe = std::make_shared<takt::Pipe<int>>("cancelled");
    pipe->set_producer({"source", 1, 1}, 0);
    pipe->set_consumer({"sink", 1, 1, 0});
    auto probe = std::make_shared<Probe>();
    takt::PipelineRuntime runtime({probe}, {pipe});
    runtime.request_stop();
    EXPECT_TRUE(pipe->closed());
    EXPECT_THROW(runtime.initialize(), std::logic_error);
    EXPECT_THROW(runtime.start(), std::logic_error);
    EXPECT_EQ(probe->starts, 0);
}

TEST(RuntimeInitialization, WholeTreeDeduplicatesIdentityNotNames)
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

TEST(RuntimeInitialization, ConfigurationFailureAllocatesNothing)
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

TEST(RuntimeInitialization, AllocationFailureIsTerminalAndRetainsCause)
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

TEST(RuntimeInitialization, FailedRuntimeDoesNotRetainPipesBeyondItsOwner)
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

TEST(RuntimeInitialization, PartialStartupStopsNodesAndPreservesFailure)
{
    auto started = std::make_shared<Probe>();
    auto failing = std::make_shared<FailingStart>();
    takt::PipelineRuntime runtime({started, failing});
    runtime.initialize();
    EXPECT_THROW(runtime.start(), std::runtime_error);
    EXPECT_EQ(started->starts, 1);
    EXPECT_TRUE(started->stop_requested());
    EXPECT_TRUE(failing->stop_requested());
    EXPECT_NO_THROW(runtime.join());
}

TEST(RuntimeInitialization, ReadyPipeCanBeAdoptedButTerminalPipeCannot)
{
    auto pipe = std::make_shared<takt::Pipe<int>>("ready");
    pipe->set_producer({"source", 1, 1}, 0);
    pipe->set_consumer({"sink", 1, 1, 0});
    pipe->initialize();
    takt::PipelineRuntime runtime({}, {pipe});
    EXPECT_NO_THROW(runtime.initialize());
    pipe->close();
    takt::PipelineRuntime terminal({}, {pipe});
    EXPECT_THROW(terminal.initialize(), std::logic_error);
}

TEST(RuntimeInitialization, EndpointWrappersReferToSamePreparedObject)
{
    auto pipe = std::make_shared<takt::Pipe<int>>("connection");
    pipe->set_producer({"source", 1, 1}, 0);
    pipe->set_consumer({"sink", 1, 1, 0});
    auto graph = takt::PipelineBuilder::create("graph");
    auto source = std::make_shared<Probe>();
    auto sink = std::make_shared<Probe>();
    graph.add_node("source").add_node("sink");
    auto output = takt::make_pipe_endpoint(pipe);
    auto input = takt::make_pipe_endpoint(pipe);
    graph.add_output_port("source", "out", {{"pipe_endpoint", output}});
    graph.add_input_port("sink", "in", {{"pipe_endpoint", input}});
    graph.connect("source", "out", "sink", "in");
    graph.bind_node("source", source).bind_node("sink", sink);
    auto runtime = graph.build_runtime();
    EXPECT_FALSE(pipe->initialized());
    runtime->initialize();
    EXPECT_EQ(output->lifecycle_pipe().get(), pipe.get());
    EXPECT_EQ(input->lifecycle_pipe().get(), pipe.get());
    EXPECT_TRUE(pipe->initialized());
    EXPECT_EQ(source->starts + sink->starts, 0);
}

TEST(RuntimeInitialization, InvalidEndpointDescriptorFailsBeforeRuntimeCreation)
{
    auto graph = takt::PipelineBuilder::create("invalid-endpoint");
    graph.add_node("node");
    graph.add_input_port("node", "in", {{"pipe_endpoint", 42}});
    graph.bind_node("node", std::make_shared<Probe>());
    EXPECT_THROW(graph.build_runtime(), std::logic_error);
}

TEST(RuntimeInitialization, CyclesAndSharedNodeOwnershipAreRejected)
{
    auto probe = std::make_shared<Probe>();
    auto runtime = std::make_shared<takt::PipelineRuntime>(
        std::vector<std::shared_ptr<takt::NodeBase>>{probe});
    probe->children = {runtime};
    EXPECT_THROW(runtime->initialize(), std::logic_error);
    probe->children.clear();
    EXPECT_EQ(probe->starts, 0);

    auto child = std::make_shared<takt::PipelineRuntime>(
        std::vector<std::shared_ptr<takt::NodeBase>>{probe});
    auto outer = std::make_shared<Probe>();
    outer->children = {child};
    takt::PipelineRuntime duplicate({outer, probe});
    EXPECT_THROW(duplicate.initialize(), std::logic_error);
    EXPECT_EQ(probe->starts + outer->starts, 0);
}

TEST(RuntimeInitialization, BridgePipesAreIncludedInInnerRuntimePreparation)
{
    auto source = std::make_shared<takt::Pipe<int>>("outer");
    auto target = std::make_shared<takt::Pipe<int>>("inner");
    for (const auto& pipe : {source, target})
    {
        pipe->set_producer({"producer", 1, 1}, 0);
        pipe->set_consumer({"consumer", 1, 1, 0});
    }
    auto inner = std::make_shared<takt::PipelineRuntime>(
        std::vector<std::shared_ptr<takt::NodeBase>>{});
    auto subgraph = std::make_shared<takt::SubgraphNode>(
        "subgraph", inner,
        std::unordered_map<std::string, takt::NodePortRef>{{"in", {"node", "in"}}},
        std::unordered_map<std::string, takt::NodePortRef>{},
        std::vector<takt::SubgraphNode::BridgeLink>{{takt::make_pipe_endpoint(source),
                                                     takt::make_pipe_endpoint(target),
                                                     "outer.in", "inner.in"}});
    takt::PipelineRuntime root({subgraph}, {source});
    root.initialize();
    EXPECT_TRUE(inner->initialized());
    EXPECT_TRUE(source->initialized());
    EXPECT_TRUE(target->initialized());
}

TEST(RuntimeInitialization, RetainedOverlapMayExtendBeyondPhysicalCapacity)
{
    takt::Pipe<int> pipe("overlap");
    pipe.set_producer({"source", 1, 2}, 0);
    pipe.set_consumer({"sink", 1, 3, 1});
    pipe.set_capacity(2);
    pipe.initialize();
    {
        auto write = pipe.acquire_write_batch(2, true);
        write.value(0) = 1;
        write.value(1) = 2;
        write.publish();
    }
    {
        auto read = pipe.acquire_read_batch(1);
        EXPECT_EQ(read.value(0), 1);
    }
    {
        auto read = pipe.acquire_configured_read_batch();
        ASSERT_EQ(read.size(), 2U);
        EXPECT_EQ(read.value(0), 1);
        EXPECT_EQ(read.value(1), 2);
    }
}
} // namespace
