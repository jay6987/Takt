// Defines typed pipe acquisition and exception-safe replay.
#pragma once

#include "takt/pipeline/pipe_read_token.h"
#include "takt/pipeline/pipe_write_token.h"

namespace takt
{
template <typename T> typename Pipe<T>::WriteToken Pipe<T>::acquire_write()
{
    auto token =
        acquire_write_reservation(producer_config().frames_per_write, false);
    return WriteToken(std::move(token));
}

template <typename T>
typename Pipe<T>::WriteToken Pipe<T>::acquire_shot_end_write(size_t frame_count)
{
    auto token = acquire_write_reservation(frame_count, true);
    return WriteToken(std::move(token));
}

template <typename T> typename Pipe<T>::ReadToken Pipe<T>::acquire_read()
{
    require_initialized();
    const auto& config = consumer_config();
    auto window = core_->acquire_read_window(config.frames_per_read,
                                             config.overlap_frames);
    try
    {
        recordreplay::detail::RecordReplayRegistry::instance().replay_slots(
            *this, window.slots, frames_);
        std::vector<size_t> slots;
        slots.reserve(window.frame_count);
        for (size_t i = 0; i < window.frame_count; ++i)
            slots.push_back((window.start_frame_index + i) % slot_capacity());
        return ReadToken(this, window.token_id, window.is_shot_start,
                         window.is_shot_end, std::move(slots),
                         window.overlap_frames, window.frame_count);
    }
    catch (...)
    {
        core_->release_read_window(window.token_id);
        throw;
    }
}

template <typename T>
typename Pipe<T>::WriteReservation
Pipe<T>::acquire_write_reservation(size_t frame_count,
                                   bool is_shot_end)
{
    require_initialized();
    auto reservation = core_->reserve_write(frame_count, is_shot_end);
    return WriteReservation(this, reservation.token_id,
                            reservation.is_shot_start, reservation.is_shot_end,
                            std::move(reservation.slots));
}
} // namespace takt
