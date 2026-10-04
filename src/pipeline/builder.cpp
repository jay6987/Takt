#include "takt/pipeline/builder.h"

#include <algorithm>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>

namespace takt
{
namespace
{
struct ProducerSpec
{
    std::string producer_name;
    size_t producer_count = 0;
    size_t write_size = 0;
    std::any basket_template;
    std::type_index basket_type{typeid(void)};
};

struct ConsumerSpec
{
    std::string consumer_name;
    size_t consumer_count = 0;
    size_t read_size = 0;
    size_t overlap_size = 0;
};

} // namespace

struct PipeBuilder::State
{
    explicit State(std::string name) : pipe_name(std::move(name)) {}

    std::string pipe_name;
    std::optional<ProducerSpec> producer;
    std::optional<ConsumerSpec> consumer;
    PipeDescriptor descriptor;
    std::optional<size_t> explicit_capacity;
    BufferPolicy policy = BufferPolicy::Minimal;
    mutable std::mutex mutex;
};

namespace
{

class Registry
{
  public:
    static Registry& instance()
    {
        static Registry registry;
        return registry;
    }

    std::shared_ptr<PipeBuilder::State>
    get_or_create_state(const std::string& pipe_name)
    {
        std::lock_guard<std::mutex> lk(mutex_);
        auto it = states_.find(pipe_name);
        if (it != states_.end())
        {
            return it->second;
        }

        auto state = std::make_shared<PipeBuilder::State>(pipe_name);
        states_.emplace(pipe_name, state);
        return state;
    }

  private:
    std::mutex mutex_;
    std::unordered_map<std::string, std::shared_ptr<PipeBuilder::State>> states_;
};
} // namespace

PipeBuilder PipeBuilder::declare(std::string pipe_name)
{
    return PipeBuilder(Registry::instance().get_or_create_state(pipe_name));
}

PipeBuilder::PipeBuilder(std::shared_ptr<State> state) : state_(std::move(state))
{
    if (!state_)
    {
        throw std::invalid_argument("pipe builder state cannot be null");
    }
}

PipeBuilder::PipeBuilder(const PipeBuilder&) = default;
PipeBuilder& PipeBuilder::operator=(const PipeBuilder&) = default;
PipeBuilder::PipeBuilder(PipeBuilder&&) noexcept = default;
PipeBuilder& PipeBuilder::operator=(PipeBuilder&&) noexcept = default;
PipeBuilder::~PipeBuilder() = default;

const std::string& PipeBuilder::name() const noexcept
{
    return state_->pipe_name;
}

PipeBuilder& PipeBuilder::producer(std::string producer_name, size_t producer_count,
                                   size_t write_size, std::any basket_template,
                                   std::type_index basket_type)
{
    if (producer_count == 0)
    {
        throw std::invalid_argument("producer count cannot be zero");
    }
    if (write_size == 0)
    {
        throw std::invalid_argument("write size cannot be zero");
    }

    std::lock_guard<std::mutex> lk(state_->mutex);
    if (state_->producer.has_value())
    {
        throw std::logic_error("producer is already set");
    }

    ProducerSpec spec;
    spec.producer_name = std::move(producer_name);
    spec.producer_count = producer_count;
    spec.write_size = write_size;
    spec.basket_template = std::move(basket_template);
    spec.basket_type = basket_type;
    state_->producer = std::move(spec);
    return *this;
}

PipeBuilder& PipeBuilder::consumer(std::string consumer_name, size_t consumer_count,
                                   size_t read_size, size_t overlap_size)
{
    if (consumer_count == 0)
    {
        throw std::invalid_argument("consumer count cannot be zero");
    }
    if (read_size == 0)
    {
        throw std::invalid_argument("read size cannot be zero");
    }
    if (overlap_size >= read_size)
    {
        throw std::invalid_argument("overlap size must be smaller than read size");
    }

    std::lock_guard<std::mutex> lk(state_->mutex);
    if (state_->consumer.has_value())
    {
        throw std::logic_error("consumer is already set");
    }

    ConsumerSpec spec;
    spec.consumer_name = std::move(consumer_name);
    spec.consumer_count = consumer_count;
    spec.read_size = read_size;
    spec.overlap_size = overlap_size;
    state_->consumer = std::move(spec);
    return *this;
}

PipeBuilder& PipeBuilder::descriptor(PipeDescriptor descriptor)
{
    std::lock_guard<std::mutex> lk(state_->mutex);
    state_->descriptor = std::move(descriptor);
    return *this;
}

PipeBuilder& PipeBuilder::descriptor_value(std::string key, std::any value)
{
    std::lock_guard<std::mutex> lk(state_->mutex);
    state_->descriptor[std::move(key)] = std::move(value);
    return *this;
}

PipeBuilder& PipeBuilder::buffer_size(size_t capacity)
{
    if (capacity == 0)
    {
        throw std::invalid_argument("buffer size cannot be zero");
    }

    std::lock_guard<std::mutex> lk(state_->mutex);
    state_->explicit_capacity = capacity;
    return *this;
}

PipeBuilder& PipeBuilder::minimal_buffer()
{
    std::lock_guard<std::mutex> lk(state_->mutex);
    state_->policy = BufferPolicy::Minimal;
    return *this;
}

PipeBuilder& PipeBuilder::max_concurrency_buffer()
{
    std::lock_guard<std::mutex> lk(state_->mutex);
    state_->policy = BufferPolicy::MaxConcurrency;
    return *this;
}

bool PipeBuilder::has_producer() const noexcept
{
    std::lock_guard<std::mutex> lk(state_->mutex);
    return state_->producer.has_value();
}

bool PipeBuilder::has_consumer() const noexcept
{
    std::lock_guard<std::mutex> lk(state_->mutex);
    return state_->consumer.has_value();
}

size_t PipeBuilder::capacity() const
{
    std::lock_guard<std::mutex> lk(state_->mutex);
    if (state_->explicit_capacity.has_value())
    {
        return *state_->explicit_capacity;
    }
    if (!state_->producer.has_value())
    {
        throw std::logic_error("producer spec is missing");
    }
    if (!state_->consumer.has_value())
    {
        throw std::logic_error("consumer spec is missing");
    }

    const auto& producer = *state_->producer;
    const auto& consumer = *state_->consumer;
    const size_t write_need = producer.producer_count * producer.write_size;
    const size_t read_need =
        consumer.consumer_count * (consumer.read_size - consumer.overlap_size) +
        consumer.overlap_size;

    switch (state_->policy)
    {
    case BufferPolicy::Minimal:
        return std::max(write_need, read_need);
    case BufferPolicy::MaxConcurrency:
        return write_need +
               consumer.consumer_count * (consumer.read_size - consumer.overlap_size) +
               consumer.overlap_size;
    }

    throw std::logic_error("unknown buffer policy");
}

std::type_index PipeBuilder::basket_type() const
{
    std::lock_guard<std::mutex> lk(state_->mutex);
    if (!state_->producer.has_value())
    {
        throw std::logic_error("producer spec is missing");
    }

    return state_->producer->basket_type;
}

const std::any& PipeBuilder::basket_template() const
{
    std::lock_guard<std::mutex> lk(state_->mutex);
    if (!state_->producer.has_value())
    {
        throw std::logic_error("producer spec is missing");
    }
    if (!state_->producer->basket_template.has_value())
    {
        throw std::logic_error("producer basket template is missing");
    }

    return state_->producer->basket_template;
}

PipeDescriptor PipeBuilder::descriptor() const
{
    std::lock_guard<std::mutex> lk(state_->mutex);
    return state_->descriptor;
}
} // namespace takt
