// Demonstrates a nested increment pipeline with bound subgraph ports.
#include "double_node.h"

#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include "takt/takt.h"

namespace
{
// Configures outer boundaries for either direct or bridged node mappings.
void configure_outer_pipes(bool bridge,
                           std::shared_ptr<takt::Pipe<int>>& input,
                           std::shared_ptr<takt::Pipe<int>>& output)
{
    input = std::make_shared<takt::Pipe<int>>("outer-in");
    input->set_producer(
        {"source", takt::concurrency(1), takt::frames_per_write(1)}, 0);
    input->set_consumer({bridge ? "input-bridge" : "double",
                         takt::concurrency(1), takt::frames_per_read(1),
                         takt::overlap_frames(0)});
    output = std::make_shared<takt::Pipe<int>>("outer-out");
    output->set_producer({bridge ? "output-bridge" : "double",
                          takt::concurrency(1), takt::frames_per_write(1)},
                         0);
    output->set_consumer({"sink", takt::concurrency(1),
                          takt::frames_per_read(1), takt::overlap_frames(0)});
    input->set_slot_capacity(16);
    output->set_slot_capacity(16);
}
// Configures independent inner boundary pipes when bridge mode is selected.
void configure_bridge_pipes(std::shared_ptr<takt::Pipe<int>>& input,
                            std::shared_ptr<takt::Pipe<int>>& output)
{
    input = std::make_shared<takt::Pipe<int>>("inner-in");
    input->set_producer(
        {"input-bridge", takt::concurrency(1), takt::frames_per_write(1)}, 0);
    input->set_consumer({"double", takt::concurrency(1),
                         takt::frames_per_read(1), takt::overlap_frames(0)});
    output = std::make_shared<takt::Pipe<int>>("inner-out");
    output->set_producer(
        {"double", takt::concurrency(1), takt::frames_per_write(1)}, 0);
    output->set_consumer({"output-bridge", takt::concurrency(1),
                          takt::frames_per_read(1), takt::overlap_frames(0)});
    input->set_slot_capacity(16);
    output->set_slot_capacity(16);
}
// Binds the doubling worker and its typed ports into the inner graph.
takt::PipelineBuilder
make_inner_pipeline(const std::shared_ptr<takt::Pipe<int>>& input,
                    const std::shared_ptr<takt::Pipe<int>>& output)
{
    auto inner = takt::PipelineBuilder::create("inner");
    inner.add_node("worker");
    inner.add_input_port(
        "worker", "in",
        {{"pipe_endpoint", takt::make_pipe_endpoint<int>(input)}});
    inner.add_output_port(
        "worker", "out",
        {{"pipe_endpoint", takt::make_pipe_endpoint<int>(output)}});
    inner.bind_node("worker",
                    std::make_shared<takt_example::DoubleNode>(input, output));
    return inner;
}
// Maps outer ports onto the inner worker's input and output boundaries.
takt::PipelineBuilder
make_outer_pipeline(const takt::PipelineBuilder& inner,
                    const std::shared_ptr<takt::Pipe<int>>& input,
                    const std::shared_ptr<takt::Pipe<int>>& output)
{
    auto outer = takt::PipelineBuilder::create("outer");
    outer.add_node("sg");
    outer.add_input_port(
        "sg", "raw", {{"pipe_endpoint", takt::make_pipe_endpoint<int>(input)}});
    outer.add_output_port(
        "sg", "result",
        {{"pipe_endpoint", takt::make_pipe_endpoint<int>(output)}});
    outer.bind_subgraph("sg", inner, {{"raw", {"worker", "in"}}},
                        {{"result", {"worker", "out"}}});
    return outer;
}
// Publishes four ordered shot-ending frames and closes their source.
void publish_inputs(takt::Pipe<int>& input)
{
    for (int value = 1; value <= 4; ++value)
    {
        auto write = input.acquire_shot_end_write(1);
        write.value() = value;
        write.publish();
    }
    input.close();
}
// Collects transformed output values until normal downstream completion.
std::vector<int> drain_outputs(takt::Pipe<int>& output)
{
    std::vector<int> result;
    while (true)
    {
        try
        {
            auto read = output.acquire_read();
            result.push_back(read.value());
        }
        catch (const takt::PipeClosedAndEmptySignal&)
        {
            return result;
        }
    }
}
} // namespace

// Executes direct or bridged nesting and verifies the resulting frame values.
int main(int argc,
         char** argv)
{
    const bool bridge = argc == 2 && std::string(argv[1]) == "bridge";
    if (argc > 2 || (argc == 2 && !bridge))
    {
        std::cerr << "usage: takt_subgraph_example [bridge]\n";
        return 1;
    }
    std::shared_ptr<takt::Pipe<int>> outer_input;
    std::shared_ptr<takt::Pipe<int>> outer_output;
    configure_outer_pipes(bridge, outer_input, outer_output);
    auto inner_input = outer_input;
    auto inner_output = outer_output;
    if (bridge)
    {
        configure_bridge_pipes(inner_input, inner_output);
    }

    auto inner = make_inner_pipeline(inner_input, inner_output);
    auto outer = make_outer_pipeline(inner, outer_input, outer_output);
    auto runtime = outer.build_runtime();
    runtime->initialize();
    runtime->start();
    publish_inputs(*outer_input);
    runtime->join();

    const auto result = drain_outputs(*outer_output);
    std::cout << "subgraph outputs:";
    for (int value : result)
    {
        std::cout << " " << value;
    }
    std::cout << std::endl;
    return result == std::vector<int>{2, 4, 6, 8} ? 0 : 1;
}
