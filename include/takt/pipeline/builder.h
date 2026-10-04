#pragma once

#include <any>
#include <cstddef>
#include <memory>
#include <stdexcept>
#include <string>
#include <typeindex>
#include <utility>

#include "takt/pipeline/descriptor.h"
#include "takt/pipeline/pipe.h"

namespace takt
{
enum class BufferPolicy
{
    Minimal,
    MaxConcurrency,
};

class PipeBuilder
{
  public:
    static PipeBuilder declare(std::string pipe_name);

    PipeBuilder(const PipeBuilder&);
    PipeBuilder& operator=(const PipeBuilder&);
    PipeBuilder(PipeBuilder&&) noexcept;
    PipeBuilder& operator=(PipeBuilder&&) noexcept;
    ~PipeBuilder();

    const std::string& name() const noexcept;

    PipeBuilder& producer(std::string producer_name, size_t producer_count,
                          size_t write_size, std::any basket_template,
                          std::type_index basket_type);

    template <typename T>
    PipeBuilder& producer(std::string producer_name, size_t producer_count,
                          size_t write_size, T basket_template)
    {
        return producer(std::move(producer_name), producer_count, write_size,
                        std::any(std::move(basket_template)),
                        std::type_index(typeid(T)));
    }

    PipeBuilder& consumer(std::string consumer_name, size_t consumer_count,
                          size_t read_size, size_t overlap_size = 0);

    PipeBuilder& descriptor(PipeDescriptor descriptor);
    PipeBuilder& descriptor_value(std::string key, std::any value);

    PipeBuilder& buffer_size(size_t capacity);
    PipeBuilder& minimal_buffer();
    PipeBuilder& max_concurrency_buffer();

    bool has_producer() const noexcept;
    bool has_consumer() const noexcept;
    size_t capacity() const;
    std::type_index basket_type() const;
    const std::any& basket_template() const;
    PipeDescriptor descriptor() const;

    template <typename T> Pipe<T> build() const
    {
        if (!has_producer())
        {
            throw std::logic_error("producer spec is missing");
        }
        if (!has_consumer())
        {
            throw std::logic_error("consumer spec is missing");
        }
        if (basket_type() != std::type_index(typeid(T)))
        {
            throw std::logic_error("producer basket template type mismatch");
        }

        return Pipe<T>(name(), capacity(), std::any_cast<const T&>(basket_template()),
                       descriptor());
    }

    struct State;

  private:
    explicit PipeBuilder(std::shared_ptr<State> state);

    std::shared_ptr<State> state_;
};
} // namespace takt
