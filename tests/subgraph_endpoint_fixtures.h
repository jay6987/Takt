// Supplies custom endpoints and initialization/startup probes for subgraphs.
#pragma once
#include "subgraph_direct_tests_test_support_common.h"
namespace
{

/// Represents the custom endpoint test fixture.
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

/// Represents the counting int pipe test fixture.
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

/// Represents the failing node test fixture.
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
