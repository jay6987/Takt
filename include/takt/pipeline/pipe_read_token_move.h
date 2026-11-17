// Transfers read-window ownership without duplicating release responsibility.
#pragma once

#include "takt/pipeline/pipe_read_token.h"

namespace takt
{
template <typename T>
typename Pipe<T>::ReadToken&
Pipe<T>::ReadToken::operator=(ReadToken&& rhs) noexcept
{
    if (this == &rhs)
        return *this;
    release();
    owner_ = rhs.owner_;
    token_id_ = rhs.token_id_;
    is_shot_start_ = rhs.is_shot_start_;
    is_shot_end_ = rhs.is_shot_end_;
    slots_ = std::move(rhs.slots_);
    overlap_frames_ = rhs.overlap_frames_;
    frame_count_ = rhs.frame_count_;
    rhs.owner_ = nullptr;
    return *this;
}
} // namespace takt
