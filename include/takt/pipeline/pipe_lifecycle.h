// Defines typed pipe initialization and diagnostic closure.
#pragma once

#include "takt/pipeline/pipe_declaration.h"

namespace takt
{
template <typename T> void Pipe<T>::initialize()
{
    validate_configuration();
    const auto slot_count = slot_capacity();
    auto core = std::make_unique<PipeCore>(name(), slot_count,
                                           consumer_config().overlap_frames,
                                           producer_config().frames_per_write);
    std::vector<T> frames(slot_count, *frame_template_);
    core_ = std::move(core);
    frames_ = std::move(frames);
    frame_template_.reset();
    initialized_ = true;
}

template <typename T> void Pipe<T>::close_with_error(std::string message)
{
    if (message.empty())
        throw std::invalid_argument(
            context("pipe error message cannot be empty"));
    if (core_)
        core_->close_with_error(std::move(message));
    else
    {
        if (configuration_error_.empty())
            configuration_error_ = std::move(message);
        cancelled_ = true;
    }
}
} // namespace takt
