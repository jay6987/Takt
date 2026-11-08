// Tests zero-bridge direct mappings and fallback boundary transport behavior.
#include "takt/takt.h"

#include <gtest/gtest.h>

#include <chrono>
#include <future>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

namespace
{
using namespace std::chrono_literals;

// Creates a configured single-frame pipe without allocating its storage.
std::shared_ptr<takt::Pipe<int>> pipe(const std::string& name,
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
takt::PipelineBuilder
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
takt::PipelineBuilder wrap(const takt::PipelineBuilder& inner,
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

class CustomEndpoint final : public takt::AnyPipeEndpoint
{
  public:
    // Retains an optional backing pipe for endpoint capability tests.
    explicit CustomEndpoint(std::shared_ptr<takt::Pipe<int>> pipe)
        : pipe_(std::move(pipe))
    {
    }
    // Counts reads before forwarding to a standard typed adapter.
    takt::AnyPipePacket read() override
    {
        ++reads;
        return takt::make_pipe_endpoint(pipe_)->read();
    }
    // Counts writes before forwarding to a standard typed adapter.
    void write(const takt::AnyPipePacket& packet) override
    {
        ++writes;
        takt::make_pipe_endpoint(pipe_)->write(packet);
    }
    // Closes the backing pipe.
    void close() override
    {
        pipe_->close();
    }
    // Closes the backing pipe with a diagnostic fault.
    void close_with_error(std::string message) override
    {
        pipe_->close_with_error(std::move(message));
    }
    // Returns backing pipe identity or a null-adapter diagnostic label.
    std::string debug_name() const override
    {
        return pipe_ ? pipe_->name() : "null";
    }
    // Exposes backing identity for runtime preparation and classification.
    std::shared_ptr<takt::PipeBase> lifecycle_pipe() const override
    {
        return pipe_;
    }
    // Accepts shared custom adapters only after explicit capability opt-in.
    bool supports_direct_to(
        const takt::AnyPipeEndpoint& other) const noexcept override
    {
        const auto* custom = dynamic_cast<const CustomEndpoint*>(&other);
        return direct_supported && custom && pipe_ == custom->pipe_;
    }
    bool direct_supported = false;
    int reads = 0;
    int writes = 0;

  private:
    std::shared_ptr<takt::Pipe<int>> pipe_;
};

class CountingIntPipe final : public takt::Pipe<int>
{
  public:
    // Configures a typed pipe whose initialization count remains observable.
    explicit CountingIntPipe(const std::string& name) : Pipe(name)
    {
        set_producer(
            {"producer", takt::concurrency(1), takt::frames_per_write(1)}, 0);
        set_consumer({"consumer", takt::concurrency(1),
                      takt::frames_per_read(1), takt::overlap_frames(0)});
    }
    // Counts each successful allocation of typed frame storage.
    void initialize() override
    {
        Pipe::initialize();
        ++initializations;
    }
    size_t initializations = 0;
};

class FailingNode final : public takt::NodeBase
{
  public:
    // Creates the node used to inject partial-startup failure.
    FailingNode() : NodeBase("fail") {}
    // Injects deterministic failure during runtime startup.
    void start() override
    {
        throw std::runtime_error("direct startup failure");
    }
    // Completes immediately because startup never launches workers.
    void join() override {}
};
} // namespace

// Verifies direct transport requires shared identity and mutual capability.
TEST(SubgraphDirect,
     IdentityAndCustomEndpointCapability)
{
    auto first = pipe("same");
    auto second = pipe("same");
    const auto source = takt::make_pipe_endpoint(first);
    const auto alias = takt::make_pipe_endpoint(first);
    const auto other = takt::make_pipe_endpoint(second);
    EXPECT_NE(source, alias);
    EXPECT_EQ(takt::classify_mapping(*source, *alias).mode,
              takt::MappingTransport::Direct);
    EXPECT_EQ(takt::classify_mapping(*source, *other).mode,
              takt::MappingTransport::Bridge);
    CustomEndpoint custom(first);
    CustomEndpoint custom_other(second);
    CustomEndpoint null(nullptr);
    EXPECT_THROW(takt::classify_mapping(*source, custom),
                 std::invalid_argument);
    EXPECT_THROW(takt::classify_mapping(custom, custom), std::invalid_argument);
    EXPECT_THROW(takt::classify_mapping(*source, null), std::invalid_argument);
    EXPECT_EQ(takt::classify_mapping(custom, custom_other).mode,
              takt::MappingTransport::Bridge);
    CustomEndpoint custom_alias(first);
    custom.direct_supported = true;
    EXPECT_THROW(takt::classify_mapping(custom, custom_alias),
                 std::invalid_argument);
    custom_alias.direct_supported = true;
    EXPECT_EQ(takt::classify_mapping(custom, custom_alias).mode,
              takt::MappingTransport::Direct);
    first->initialize();
    second->initialize();
    {
        auto write = first->acquire_write();
        write.value() = 7;
        write.publish();
    }
    custom.transfer_to(custom_other);
    EXPECT_EQ(custom.reads, 1);
    EXPECT_EQ(custom_other.writes, 1);
    EXPECT_EQ(second->acquire_read().value(), 7);
    EXPECT_THROW(source->transfer_to(*alias), std::invalid_argument);
}

// Verifies direct and mixed boundaries both preserve ordered frame delivery.
TEST(SubgraphDirect,
     AllDirectAndMixedMappingsTransferOrderedFrames)
{
    for (const size_t capacity : {1U, 2U})
        for (const bool bridge_input : {false, true})
            for (const bool bridge_output : {false, true})
            {
                auto input = pipe("input", capacity);
                auto output = pipe("output", capacity);
                auto inner_input =
                    bridge_input ? pipe("inner-input", capacity) : input;
                auto inner_output =
                    bridge_output ? pipe("inner-output", capacity) : output;
                auto inner = relay_graph(inner_input, inner_output);
                auto outer = wrap(inner, input, output);
                auto node = std::dynamic_pointer_cast<takt::SubgraphNode>(
                    outer.find_bound_node("group"));
                ASSERT_NE(node, nullptr);
                EXPECT_EQ(node->mapping_count(), 2U);
                EXPECT_EQ(node->bridge_count(),
                          static_cast<size_t>(bridge_input) +
                              static_cast<size_t>(bridge_output));
                const auto snapshot = outer.graph().subgraphs.at("group");
                EXPECT_EQ(snapshot.input_transports.at("in").mode,
                          bridge_input ? takt::MappingTransport::Bridge
                                       : takt::MappingTransport::Direct);
                EXPECT_EQ(snapshot.output_transports.at("out").mode,
                          bridge_output ? takt::MappingTransport::Bridge
                                        : takt::MappingTransport::Direct);
                auto runtime = outer.build_runtime();
                runtime->initialize();
                runtime->start();
                EXPECT_EQ(node->launched_bridge_worker_count(),
                          node->bridge_count());
                auto writer =
                    std::async(std::launch::async,
                               [&]
                               {
                                   for (int frame = 0; frame < 16; ++frame)
                                   {
                                       auto write =
                                           input->acquire_shot_end_write(1);
                                       write.value() = frame;
                                       write.publish();
                                   }
                                   input->close();
                               });
                auto reader =
                    std::async(std::launch::async,
                               [&]
                               {
                                   std::vector<int> values;
                                   try
                                   {
                                       while (true)
                                       {
                                           auto read = output->acquire_read();
                                           values.push_back(read.value());
                                       }
                                   }
                                   catch (const takt::PipeClosedAndEmptySignal&)
                                   {
                                   }
                                   return values;
                               });
                const auto status = reader.wait_for(5s);
                if (status != std::future_status::ready)
                    runtime->request_stop();
                EXPECT_EQ(status, std::future_status::ready);
                writer.get();
                const auto values = reader.get();
                ASSERT_EQ(values.size(), 16U);
                for (size_t frame = 0; frame < values.size(); ++frame)
                    EXPECT_EQ(values[frame], static_cast<int>(frame) * 2);
                runtime->join();
            }
}

// Verifies direct construction registers lifecycle pipes and complete pairs.
TEST(SubgraphDirect,
     DirectConstructionRegistersPipesAndRejectsMissingPairs)
{
    auto shared = pipe("shared");
    auto inner = std::make_shared<takt::PipelineRuntime>(
        std::vector<std::shared_ptr<takt::NodeBase>>{});
    const std::unordered_map<std::string, takt::NodePortRef> mappings{
        {"in", {"worker", "in"}}};
    EXPECT_THROW((takt::SubgraphNode("bad", inner, mappings)),
                 std::logic_error);
    EXPECT_THROW((takt::SubgraphNode(
                     "bad", inner, mappings, {},
                     {{nullptr, takt::make_pipe_endpoint(shared), "", ""}})),
                 std::invalid_argument);
    takt::SubgraphNode node(
        "direct", inner, mappings, {},
        {{takt::make_pipe_endpoint(shared), takt::make_pipe_endpoint(shared),
          "outer.in", "worker.in"}});
    EXPECT_THROW(node.start(), std::logic_error);
    inner->initialize();
    EXPECT_TRUE(shared->initialized());
    EXPECT_THROW(shared->initialize(), std::logic_error);
    node.start();
    EXPECT_EQ(node.bridge_count(), 0U);
    EXPECT_EQ(node.launched_bridge_worker_count(), 0U);
    node.request_stop();
    node.request_stop();
    node.join();
    EXPECT_TRUE(shared->closed());
}

// Verifies direct boundaries propagate empty completion and upstream faults.
TEST(SubgraphDirect,
     EmptyInputAndFaultedPrefixPropagateWithoutBridge)
{
    for (const bool fault : {false, true})
    {
        auto input = pipe("input");
        auto output = pipe("output");
        auto graph = wrap(relay_graph(input, output), input, output);
        auto runtime = graph.build_runtime();
        runtime->initialize();
        if (fault)
        {
            auto write = input->acquire_write();
            write.value() = 21;
            write.publish();
        }
        if (fault)
            input->close_with_error("source fault");
        else
            input->close();
        runtime->start();
        auto result =
            std::async(std::launch::async,
                       [&]
                       {
                           if (fault)
                               EXPECT_EQ(output->acquire_read().value(), 42);
                           if (fault)
                               EXPECT_THROW(output->acquire_read(),
                                            takt::PipeFaultedError);
                           else
                               EXPECT_THROW(output->acquire_read(),
                                            takt::PipeClosedAndEmptySignal);
                       });
        const auto status = result.wait_for(5s);
        if (status != std::future_status::ready)
            runtime->request_stop();
        EXPECT_EQ(status, std::future_status::ready);
        result.get();
        if (fault)
            EXPECT_THROW(runtime->join(), takt::PipeFaultedError);
        else
            EXPECT_NO_THROW(runtime->join());
    }
}

// Verifies nested direct pipes initialize and are closed after startup failure.
TEST(SubgraphDirect,
     NestedDirectInitializationAndStartupCleanup)
{
    for (const bool fail_start : {false, true})
    {
        auto input = pipe("input");
        auto output = pipe("output");
        auto middle = wrap(relay_graph(input, output), input, output);
        auto outer = takt::PipelineBuilder::create("outer");
        outer.add_node("nested")
            .add_input_port(
                "nested", "in",
                {{"pipe_endpoint", takt::make_pipe_endpoint(input)}})
            .add_output_port(
                "nested", "out",
                {{"pipe_endpoint", takt::make_pipe_endpoint(output)}})
            .bind_subgraph("nested", middle, {{"in", {"group", "in"}}},
                           {{"out", {"group", "out"}}});
        auto runtime = outer.build_runtime();
        if (!fail_start)
            runtime->initialize();
        else
            EXPECT_THROW(runtime->start(), std::logic_error);
        if (fail_start)
        {
            runtime->request_stop();
            EXPECT_TRUE(input->closed());
            EXPECT_THROW(runtime->initialize(), std::logic_error);
        }

        else
        {
            runtime->start();
            runtime->request_stop();
            EXPECT_NO_THROW(runtime->join());
            EXPECT_TRUE(input->closed());
            EXPECT_TRUE(output->closed());
        }
    }
}

// Verifies shared direct pipes allocate storage once across nested runtimes.
TEST(SubgraphDirect,
     SharedTypedPipesInitializeOnceAcrossNestedRuntimeTree)
{
    auto input = std::make_shared<CountingIntPipe>("same");
    auto output = std::make_shared<CountingIntPipe>("same");
    auto graph = wrap(relay_graph(input, output), input, output);
    auto root = graph.build_runtime();
    root->initialize();
    EXPECT_EQ(input->initializations, 1U);
    EXPECT_EQ(output->initializations, 1U);
    EXPECT_EQ(input->producer_config().name, "producer");
    EXPECT_EQ(input->consumer_config().name, "consumer");
    root->request_stop();
}

// Verifies partial startup cancellation closes shared direct boundary pipes.
TEST(SubgraphDirect,
     PartialStartupFailureClosesDirectBoundaries)
{
    auto input = pipe("input");
    auto output = pipe("output");
    auto inner = relay_graph(input, output);
    inner.add_node("fail").bind_node("fail", std::make_shared<FailingNode>());
    auto outer = wrap(inner, input, output);
    auto root = outer.build_runtime();
    root->initialize();
    EXPECT_THROW(root->start(), std::runtime_error);
    EXPECT_TRUE(input->closed());
    EXPECT_TRUE(output->closed());
    EXPECT_NO_THROW(root->join());
}

// Verifies stopping direct nesting releases both blocked endpoint operations.
TEST(SubgraphDirect,
     StopReleasesBlockedProducerAndConsumer)
{
    for (const bool write_frames : {false, true})
    {
        auto input = pipe("input");
        auto output = pipe("output");
        auto graph = wrap(relay_graph(input, output), input, output);
        auto root = graph.build_runtime();
        root->initialize();
        root->start();
        std::promise<void> entered;
        auto ready = entered.get_future();
        auto participant =
            std::async(std::launch::async,
                       [&]
                       {
                           entered.set_value();
                           try
                           {
                               if (write_frames)
                                   for (int frame = 0; frame < 64; ++frame)
                                   {
                                       auto write =
                                           input->acquire_shot_end_write(1);
                                       write.value() = frame;
                                       write.publish();
                                   }
                               else
                                   (void)output->acquire_read();
                           }
                           catch (const takt::PipeClosedAndEmptySignal&)
                           {
                           }
                       });
        const auto status = ready.wait_for(5s);
        EXPECT_EQ(status, std::future_status::ready);
        EXPECT_EQ(participant.wait_for(20ms), std::future_status::timeout);
        root->request_stop();
        EXPECT_EQ(participant.wait_for(5s), std::future_status::ready);
        participant.get();
        EXPECT_NO_THROW(root->join());
    }
}
