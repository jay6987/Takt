#pragma once

#include "takt/takt.h"

#include <memory>
#include <utility>

template <typename T>
void configure_pipe(takt::Pipe<T>& pipe, size_t capacity, T basket_template,
                    takt::ReadHistoryPolicy history = takt::ReadHistoryPolicy::Snapshot)
{
    pipe.set_producer({"test-producer", 1, 1}, std::move(basket_template));
    pipe.set_consumer({"test-consumer", 1, 1, 0});
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
