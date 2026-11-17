// Verifies runtime startup failure cleanup, endpoint validation, and ownership.
#include "runtime_initialization_tests_test_support.h"

// Verifies partial startup stops earlier nodes and reports the starting
// failure.
TEST(RuntimeInitialization,
     PartialStartupStopsNodesAndPreservesFailure)
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

// Verifies runtime adoption accepts initialized pipes but rejects closed pipes.
TEST(RuntimeInitialization,
     ReadyPipeCanBeAdoptedButTerminalPipeCannot)
{
    auto pipe = std::make_shared<takt::Pipe<int>>("ready");
    pipe->set_producer(
        {"source", takt::concurrency(1), takt::frames_per_write(1)}, 0);
    pipe->set_consumer({"sink", takt::concurrency(1), takt::frames_per_read(1),
                        takt::overlap_frames(0)});
    pipe->initialize();
    takt::PipelineRuntime runtime({}, {pipe});
    EXPECT_NO_THROW(runtime.initialize());
    pipe->close();
    takt::PipelineRuntime terminal({}, {pipe});
    EXPECT_THROW(terminal.initialize(), std::logic_error);
}

// Verifies stop attempts every pipe closure before propagating a shot error.
TEST(RuntimeInitialization,
     StopClosesEveryPipeBeforeReportingUnterminatedShot)
{
    auto first = std::make_shared<takt::Pipe<int>>("unfinished-first");
    auto second = std::make_shared<takt::Pipe<int>>("unfinished-second");
    for (const auto& pipe : {first, second})
    {
        pipe->set_producer(
            {"source", takt::concurrency(1), takt::frames_per_write(1)}, 0);
        pipe->set_consumer({"sink", takt::concurrency(1),
                            takt::frames_per_read(1), takt::overlap_frames(0)});
        pipe->set_slot_capacity(2);
        pipe->initialize();
        auto write = pipe->acquire_write();
        write.value() = 7;
        write.publish();
    }

    takt::PipelineRuntime runtime({}, {first, second});
    runtime.initialize();
    EXPECT_THROW(runtime.request_stop(), takt::PipeUnterminatedShotError);
    EXPECT_TRUE(first->closed());
    EXPECT_TRUE(second->closed());
    EXPECT_NO_THROW(runtime.join());
}

// Verifies multiple adapters retain the same initialized backing pipe.
TEST(RuntimeInitialization,
     EndpointWrappersReferToSamePreparedObject)
{
    auto pipe = std::make_shared<takt::Pipe<int>>("connection");
    pipe->set_producer(
        {"source", takt::concurrency(1), takt::frames_per_write(1)}, 0);
    pipe->set_consumer({"sink", takt::concurrency(1), takt::frames_per_read(1),
                        takt::overlap_frames(0)});
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

// Verifies invalid endpoint metadata prevents executable runtime construction.
TEST(RuntimeInitialization,
     InvalidEndpointDescriptorFailsBeforeRuntimeCreation)
{
    auto graph = takt::PipelineBuilder::create("invalid-endpoint");
    graph.add_node("node");
    graph.add_input_port("node", "in", {{"pipe_endpoint", 42}});
    graph.bind_node("node", std::make_shared<Probe>());
    EXPECT_THROW(graph.build_runtime(), std::logic_error);
}

// Verifies executable runtime trees reject cycles and shared node ownership.
TEST(RuntimeInitialization,
     CyclesAndSharedNodeOwnershipAreRejected)
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
