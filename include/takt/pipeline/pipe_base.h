#pragma once

#include <algorithm>
#include <limits>
#include <optional>
#include <stdexcept>
#include <utility>

#include "takt/pipeline/pipe_config.h"

namespace takt
{
class PipeBase
{
  public:
    explicit PipeBase(std::string name) : name_(std::move(name)) {}
    virtual ~PipeBase() = default;
    PipeBase(const PipeBase&) = delete;
    PipeBase& operator=(const PipeBase&) = delete;

    const std::string& name() const noexcept
    {
        return name_;
    }
    bool initialized() const noexcept
    {
        return initialized_;
    }
    bool has_producer() const noexcept
    {
        return producer_.has_value();
    }
    bool has_consumer() const noexcept
    {
        return consumer_.has_value();
    }
    virtual bool closed() const noexcept = 0;
    virtual bool faulted() const noexcept = 0;
    virtual void close() = 0;
    virtual void initialize() = 0;
    virtual void validate_configuration() const = 0;

    void set_consumer(ConsumerConfig config)
    {
        require_configuring();
        if (consumer_)
            fail("consumer is already configured");
        if (config.concurrency == 0 || config.read_size == 0 ||
            config.overlap_size >= config.read_size)
            throw std::invalid_argument(context("invalid consumer configuration"));
        consumer_ = std::move(config);
    }

    void set_capacity(size_t capacity)
    {
        require_configuring();
        if (capacity == 0)
            throw std::invalid_argument(context("capacity cannot be zero"));
        explicit_capacity_ = capacity;
    }

    void request_max_concurrency()
    {
        require_configuring();
        policy_ = BufferPolicy::MaxConcurrency;
    }

    const ProducerConfig& producer_config() const
    {
        if (!producer_)
            fail("producer configuration is missing");
        return *producer_;
    }

    const ConsumerConfig& consumer_config() const
    {
        if (!consumer_)
            fail("consumer configuration is missing");
        return *consumer_;
    }

    size_t capacity() const
    {
        if (explicit_capacity_)
            return *explicit_capacity_;
        const auto& producer = producer_config();
        const auto& consumer = consumer_config();
        const auto write = multiply(producer.concurrency, producer.write_size);
        const auto read = add(
            multiply(consumer.concurrency, consumer.read_size - consumer.overlap_size),
            consumer.overlap_size);
        return policy_ == BufferPolicy::Minimal ? std::max(write, read)
                                                : add(write, read);
    }

  protected:
    void require_configuring() const
    {
        if (initialized_)
            fail("configuration is frozen after initialization");
        if (cancelled_)
            fail("configuration was cancelled");
    }

    void require_initialized() const
    {
        if (!initialized_)
            fail("pipe is not initialized");
    }

    void validate_common() const
    {
        require_configuring();
        (void)producer_config();
        (void)consumer_config();
        (void)capacity();
    }

    void set_producer_config(ProducerConfig config)
    {
        require_configuring();
        if (producer_)
            fail("producer is already configured");
        if (config.concurrency == 0 || config.write_size == 0)
            throw std::invalid_argument(context("invalid producer configuration"));
        producer_ = std::move(config);
    }

    std::string context(const std::string& message) const
    {
        return "pipe='" + name_ + "': " + message;
    }

    [[noreturn]] void fail(const std::string& message) const
    {
        throw std::logic_error(context(message));
    }

    bool initialized_ = false;
    bool cancelled_ = false;

  private:
    size_t multiply(size_t a, size_t b) const
    {
        if (b != 0 && a > std::numeric_limits<size_t>::max() / b)
            throw std::overflow_error(context("capacity multiplication overflow"));
        return a * b;
    }

    size_t add(size_t a, size_t b) const
    {
        if (a > std::numeric_limits<size_t>::max() - b)
            throw std::overflow_error(context("capacity addition overflow"));
        return a + b;
    }

    std::string name_;
    std::optional<ProducerConfig> producer_;
    std::optional<ConsumerConfig> consumer_;
    std::optional<size_t> explicit_capacity_;
    BufferPolicy policy_ = BufferPolicy::Minimal;
};
} // namespace takt
