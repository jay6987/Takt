// Validates pipe endpoint settings and computes bounded slot capacity.
#include "takt/pipeline/pipe_base.h"

#include <algorithm>
#include <limits>

namespace takt
{
void PipeBase::set_consumer(ConsumerConfig config)
{
    require_configuring();
    if (consumer_)
        fail("consumer is already configured");
    if (config.concurrency == 0 || config.frames_per_read == 0 ||
        config.overlap_frames >= config.frames_per_read)
        throw std::invalid_argument(context("invalid consumer configuration"));
    consumer_ = std::move(config);
}

void PipeBase::set_slot_capacity(size_t slot_capacity)
{
    require_configuring();
    if (slot_capacity == 0)
        throw std::invalid_argument(context("slot capacity cannot be zero"));
    explicit_slot_capacity_ = slot_capacity;
}

void PipeBase::set_producer_config(ProducerConfig config)
{
    require_configuring();
    if (producer_)
        fail("producer is already configured");
    if (config.concurrency == 0 || config.frames_per_write == 0)
        throw std::invalid_argument(context("invalid producer configuration"));
    producer_ = std::move(config);
}

size_t PipeBase::slot_capacity() const
{
    if (explicit_slot_capacity_)
        return *explicit_slot_capacity_;
    const auto& producer = producer_config();
    const auto& consumer = consumer_config();
    const auto write_slot_demand =
        multiply(producer.concurrency, producer.frames_per_write);
    const auto read_slot_demand =
        add(multiply(consumer.concurrency,
                     consumer.frames_per_read - consumer.overlap_frames),
            consumer.overlap_frames);
    return policy_ == BufferPolicy::Minimal
               ? std::max(write_slot_demand, read_slot_demand)
               : add(write_slot_demand, read_slot_demand);
}

size_t PipeBase::multiply(size_t a,
                          size_t b) const
{
    if (b != 0 && a > std::numeric_limits<size_t>::max() / b)
        throw std::overflow_error(
            context("slot capacity multiplication overflow"));
    return a * b;
}

size_t PipeBase::add(size_t a,
                     size_t b) const
{
    if (a > std::numeric_limits<size_t>::max() - b)
        throw std::overflow_error(context("slot capacity addition overflow"));
    return a + b;
}
} // namespace takt
