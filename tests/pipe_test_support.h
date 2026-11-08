#pragma once

#include "takt/takt.h"

#include <memory>
#include <utility>

template <typename T>
void configure_pipe(takt::Pipe<T>& pipe, size_t capacity, T basket_template,
                    size_t read_size = 1, size_t overlap_size = 0,
                    size_t write_size = 1)
{
    pipe.set_producer(
        {"test-producer", takt::concurrency(1),
         takt::frames_per_write(write_size)},
        std::move(basket_template));
    pipe.set_consumer({"test-consumer", takt::concurrency(1),
                       takt::frames_per_read(read_size), takt::overlap(overlap_size)});
    pipe.set_capacity(capacity);
}

template <typename T>
std::shared_ptr<takt::Pipe<T>> make_configured_pipe(
    std::string name, size_t capacity, T basket_template, size_t read_size = 1,
    size_t overlap_size = 0, size_t write_size = 1)
{
    auto pipe = std::make_shared<takt::Pipe<T>>(std::move(name));
    configure_pipe(*pipe, capacity, std::move(basket_template), read_size,
                   overlap_size, write_size);
    return pipe;
}
