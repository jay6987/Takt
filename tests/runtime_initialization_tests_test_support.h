// Verifies runtime tree initialization, lifecycle ordering and startup errors.
#pragma once
#include "takt/takt.h"

#include <gtest/gtest.h>

#include <memory>
#include <stdexcept>
#include <vector>

namespace
{
/// Represents the probe test fixture.
class Probe : public takt::NodeBase
{
  public:
    // Creates an inert node for lifecycle-tree tests.
    Probe() : NodeBase("probe") {}
    // Records startup without launching background work.
    void start() override
    {
        ++starts;
    }
    // Completes immediately because this probe owns no workers.
    void join() override {}
    // Returns configurable child runtimes for tree-validation scenarios.
    std::vector<std::shared_ptr<takt::PipelineRuntime>>
    child_runtimes() const override
    {
        return children;
    }
    int starts = 0;
    std::vector<std::shared_ptr<takt::PipelineRuntime>> children;
};

/// Represents the failing start test fixture.
class FailingStart final : public Probe
{
  public:
    // Injects a deterministic startup failure after prior nodes start.
    void start() override
    {
        throw std::runtime_error("startup failure");
    }
};

/// Represents the counting pipe test fixture.
class CountingPipe final : public takt::PipeBase
{
  public:
    // Configures a same-name pipe to test identity-based deduplication.
    CountingPipe() : PipeBase("same-name")
    {
        set_producer_config(
            {"producer", takt::concurrency(1), takt::frames_per_write(1)});
        set_consumer({"consumer", takt::concurrency(1),
                      takt::frames_per_read(1), takt::overlap_frames(0)});
    }
    // Keeps this synthetic pipe open during lifecycle tests.
    bool closed() const noexcept override
    {
        return false;
    }
    // Reports no synthetic runtime fault.
    bool faulted() const noexcept override
    {
        return false;
    }
    // Leaves synthetic storage untouched when runtime cleanup closes pipes.
    void close() override {}
    // Synthetic pipes do not transport data; record fault notification as
    // close.
    void close_with_error(std::string) override {}
    // Validates common settings and optionally injects a configuration failure.
    void validate_configuration() const override
    {
        validate_common();
        if (invalid)
            throw std::logic_error("invalid configuration");
    }
    // Counts initialization and optionally injects an allocation failure.
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
} // namespace
