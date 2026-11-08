#pragma once

#include "takt/takt.h"

#include <memory>
#include <utility>

template <typename T>
void configure_pipe(takt::Pipe<T>& pipe, size_t capacity, T basket_template,
                    takt::ReadHistoryPolicy history = takt::ReadHistoryPolicy::Snapshot)
{
    pipe.set_producer(
        {"test-producer", takt::concurrency(1), takt::frames_per_write(1)},
        std::move(basket_template));
    pipe.set_consumer({"test-consumer", takt::concurrency(1), takt::frames_per_read(1),
                       takt::overlap(0)});
    pipe.set_capacity(capacity);
    pipe.set_read_history_policy(history);
}

template <typename T>
std::shared_ptr<takt::Pipe<T>> make_configured_pipe(
    std::string name, size_t capacity, T basket_template,
    takt::ReadHistoryPolicy history = takt::ReadHistoryPolicy::Snapshot)
{
    auto pipe = std::make_shared<takt::Pipe<T>>(std::move(name));
    configure_pipe(*pipe, capacity, std::move(basket_template), history);
    return pipe;
}
