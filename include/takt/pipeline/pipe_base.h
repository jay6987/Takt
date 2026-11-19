// Validates common pipe configuration and enforces initialization boundaries.
#pragma once

#include <optional>
#include <stdexcept>
#include <utility>

#include "takt/pipeline/pipe_config.h"

namespace takt
{
class PipeCore;

/// Common configuration, lifecycle and capacity rules for typed pipes.
class PipeBase
{
  public:
    // Establishes the diagnostic identity without initializing storage.
    explicit PipeBase(std::string name) : name_(std::move(name)) {}
    // Allows derived typed pipes to release their storage polymorphically.
    virtual ~PipeBase() = default;
    // Pipe configuration and identity are not copyable.
    PipeBase(const PipeBase&) = delete;
    // Prevents replacement of configuration owned by a live pipe.
    PipeBase& operator=(const PipeBase&) = delete;

    // Returns the pipe's diagnostic identity.
    const std::string& name() const noexcept
    {
        return name_;
    }
    // Reports whether configuration has been frozen and storage initialized.
    bool initialized() const noexcept
    {
        return initialized_;
    }
    // Reports whether producer configuration has been supplied.
    bool has_producer() const noexcept
    {
        return producer_.has_value();
    }
    // Reports whether consumer configuration has been supplied.
    bool has_consumer() const noexcept
    {
        return consumer_.has_value();
    }
    // Reports cancellation or terminal runtime closure.
    virtual bool closed() const noexcept = 0;
    // Reports whether closure is associated with an error.
    virtual bool faulted() const noexcept = 0;
    // Closes the pipe and wakes blocked operations.
    virtual void close() = 0;
    // Records terminal failure and wakes blocked readers and writers.
    virtual void close_with_error(std::string message) = 0;
    // Validates configuration and allocates typed runtime storage.
    virtual void initialize() = 0;
    // Rejects configuration that cannot be initialized safely.
    virtual void validate_configuration() const = 0;

    // Sets valid read batching and overlap before initialization.
    void set_consumer(ConsumerConfig config);

    // Overrides inferred capacity with a nonzero explicit frame-slot count.
    void set_slot_capacity(size_t slot_capacity);

    // Selects capacity sufficient for simultaneous producer/consumer demand.
    void request_max_concurrency()
    {
        require_configuring();
        policy_ = BufferPolicy::MaxConcurrency;
    }

    // Returns producer settings or rejects missing configuration.
    const ProducerConfig& producer_config() const
    {
        if (!producer_)
            fail("producer configuration is missing");
        return *producer_;
    }

    // Returns consumer settings or rejects missing configuration.
    const ConsumerConfig& consumer_config() const
    {
        if (!consumer_)
            fail("consumer configuration is missing");
        return *consumer_;
    }

    // Computes required slots with checked arithmetic unless overridden.
    size_t slot_capacity() const;

  protected:
    // Rejects configuration changes after initialization or cancellation.
    void require_configuring() const
    {
        if (initialized_)
            fail("configuration is frozen after initialization");
        if (cancelled_)
            fail("configuration was cancelled");
    }

    // Rejects runtime token operations before initialization.
    void require_initialized() const
    {
        if (!initialized_)
            fail("pipe is not initialized");
    }

    // Validates lifecycle, both endpoints and computable slot capacity.
    void validate_common() const
    {
        require_configuring();
        (void)producer_config();
        (void)consumer_config();
        (void)slot_capacity();
    }

    // Installs valid producer batching exactly once during configuration.
    void set_producer_config(ProducerConfig config);

    // Prefixes a diagnostic with the pipe identity.
    std::string context(const std::string& message) const
    {
        return "pipe='" + name_ + "': " + message;
    }

    // Throws a lifecycle error carrying pipe context.
    [[noreturn]] void fail(const std::string& message) const
    {
        throw std::logic_error(context(message));
    }

    bool initialized_ = false;
    bool cancelled_ = false;

  private:
    friend class PipelineRuntime;
    virtual PipeCore* statistics_core() noexcept { return nullptr; }

    // Multiplies capacity demands without unsigned overflow.
    size_t multiply(size_t a,
                    size_t b) const;

    // Adds capacity demands without unsigned overflow.
    size_t add(size_t a,
               size_t b) const;

    std::string name_;
    std::optional<ProducerConfig> producer_;
    std::optional<ConsumerConfig> consumer_;
    std::optional<size_t> explicit_slot_capacity_;
    BufferPolicy policy_ = BufferPolicy::Minimal;
};
} // namespace takt
