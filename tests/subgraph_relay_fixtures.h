// Supplies configured relay pipes and nested graph builders for mapping tests.
#pragma once
#include "subgraph_direct_tests_test_support_common.h"
namespace
{
using namespace std::chrono_literals;

// Creates a configured single-frame pipe without allocating its storage.
inline std::shared_ptr<takt::Pipe<int>> pipe(const std::string& name,
                                             size_t capacity = 1)
{
    auto result = std::make_shared<takt::Pipe<int>>(name);
    result->set_producer(
        {"producer", takt::concurrency(1), takt::frames_per_write(1)}, 0);
    result->set_consumer({"consumer", takt::concurrency(1),
                          takt::frames_per_read(1), takt::overlap_frames(0)});
    result->set_slot_capacity(capacity);
    return result;
}

/// Represents the relay test fixture.
class Relay final : public takt::NodeBase
{
  public:
    // Binds pipes to an asynchronous frame-doubling relay.
    Relay(std::shared_ptr<takt::Pipe<int>> input,
          std::shared_ptr<takt::Pipe<int>> output)
        : NodeBase("relay"),
          input_(std::move(input)),
          output_(std::move(output))
    {
    }

    // Launches frame transfer and propagates completion or faults to output.
    void start() override
    {
        worker_ =
            std::async(std::launch::async,
                       [this]
                       {
                           try
                           {
                               while (true)
                               {
                                   auto read = input_->acquire_read();
                                   auto write =
                                       read.is_shot_end()
                                           ? output_->acquire_shot_end_write(1)
                                           : output_->acquire_write();
                                   write.value() = read.value() * 2;
                                   write.publish();
                               }
                           }
                           catch (const takt::PipeClosedAndEmptySignal&)
                           {
                               output_->close();
                           }
                           catch (const std::exception& error)
                           {
                               input_->close_with_error(error.what());
                               output_->close_with_error(error.what());
                               throw;
                           }
                       });
    }
    // Waits for the relay and propagates transfer failures.
    void join() override
    {
        if (worker_.valid())
            worker_.get();
    }
    // Closes both boundaries to release blocked frame operations.
    void request_stop() noexcept override
    {
        input_->close();
        output_->close();
    }

  private:
    std::shared_ptr<takt::Pipe<int>> input_;
    std::shared_ptr<takt::Pipe<int>> output_;
    std::future<void> worker_;
};

// Builds an executable one-node relay with endpoint-bound ports.
inline takt::PipelineBuilder
relay_graph(const std::shared_ptr<takt::Pipe<int>>& input,
            const std::shared_ptr<takt::Pipe<int>>& output)
{
    auto graph = takt::PipelineBuilder::create("relay");
    graph.add_node("worker")
        .add_input_port("worker", "in",
                        {{"pipe_endpoint", takt::make_pipe_endpoint(input)}})
        .add_output_port("worker", "out",
                         {{"pipe_endpoint", takt::make_pipe_endpoint(output)}})
        .bind_node("worker", std::make_shared<Relay>(input, output));
    return graph;
}

// Wraps a relay graph with explicit parent boundary mappings.
inline takt::PipelineBuilder
wrap(const takt::PipelineBuilder& inner,
     const std::shared_ptr<takt::Pipe<int>>& input,
     const std::shared_ptr<takt::Pipe<int>>& output)
{
    auto graph = takt::PipelineBuilder::create("wrapper");
    graph.add_node("group")
        .add_input_port("group", "in",
                        {{"pipe_endpoint", takt::make_pipe_endpoint(input)}})
        .add_output_port("group", "out",
                         {{"pipe_endpoint", takt::make_pipe_endpoint(output)}})
        .bind_subgraph("group", inner, {{"in", {"worker", "in"}}},
                       {{"out", {"worker", "out"}}});
    return graph;
}
} // namespace
