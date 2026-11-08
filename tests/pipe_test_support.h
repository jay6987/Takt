#pragma once

#include "takt/takt.h"

#include <memory>
#include <utility>

template <typename T>
void configure_pipe(takt::Pipe<T>& pipe, size_t slot_capacity, T frame_template,
                    size_t frames_per_read = 1, size_t overlap_frames = 0,
                    size_t frames_per_write = 1)
{
    pipe.set_producer(
        {"test-producer", takt::concurrency(1),
         takt::frames_per_write(frames_per_write)},
        std::move(frame_template));
    pipe.set_consumer({"test-consumer", takt::concurrency(1),
                       takt::frames_per_read(frames_per_read), takt::overlap_frames(overlap_frames)});
    pipe.set_slot_capacity(slot_capacity);
}

template <typename T>
std::shared_ptr<takt::Pipe<T>> make_configured_pipe(
    std::string name, size_t slot_capacity, T frame_template, size_t frames_per_read = 1,
    size_t overlap_frames = 0, size_t frames_per_write = 1)
{
    auto pipe = std::make_shared<takt::Pipe<T>>(std::move(name));
    configure_pipe(*pipe, slot_capacity, std::move(frame_template), frames_per_read,
                   overlap_frames, frames_per_write);
    return pipe;
}
