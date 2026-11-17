// Validates and acquires synchronized, shot-bounded read windows.
#include "takt/pipeline/pipe_core.h"

#include <algorithm>

namespace takt
{
void PipeCore::validate_read(size_t frames_per_read,
                             size_t overlap_frames) const
{
    if (frames_per_read == 0)
        throw std::invalid_argument("read frame count cannot be zero");
    if (overlap_frames >= frames_per_read)
        throw std::invalid_argument(
            "overlap frame count must be smaller than read frame count");
    if (overlap_frames > retained_overlap_frames_)
        throw std::invalid_argument(
            "requested overlap exceeds configured retention");
}

size_t PipeCore::read_start_locked(size_t overlap_frames) const
{
    return std::max(read_shot_start_frame_index_,
                    read_reserved_frame_end_ > overlap_frames
                        ? read_reserved_frame_end_ - overlap_frames
                        : 0);
}

size_t PipeCore::read_end_locked(size_t start,
                                 size_t wanted) const
{
    const size_t visible = visible_frame_end_locked();
    return visible == 0 ? start : std::min(start + wanted, visible);
}

bool PipeCore::read_ready_locked(size_t frames_per_read,
                                 size_t overlap_frames) const
{
    if (closed_ && pending_writes_.empty())
        return true;
    if (visible_frame_end_locked() == 0)
        return false;
    const size_t start = read_start_locked(overlap_frames);
    const size_t end = read_end_locked(start, frames_per_read);
    const size_t new_count = end - read_reserved_frame_end_;
    const bool hit_shot_end = next_shot_end_frame_locked() != 0 &&
                              end == next_shot_end_frame_locked();
    return new_count >= frames_per_read - overlap_frames ||
           (hit_shot_end && new_count > 0) || (closed_ && new_count > 0);
}

PipeCoreFrameWindow PipeCore::acquire_read_window(size_t frames_per_read,
                                                  size_t overlap_frames)
{
    validate_read(frames_per_read, overlap_frames);
    std::unique_lock<std::mutex> lk(mutex_);
    const size_t available_overlap =
        std::min(overlap_frames,
                 read_reserved_frame_end_ - read_shot_start_frame_index_);
    if (frames_per_read - available_overlap > slot_capacity_)
        throw std::invalid_argument(
            "new read slots cannot exceed pipe capacity");
    cv_.wait(lk, [this, frames_per_read, overlap_frames]
             { return read_ready_locked(frames_per_read, overlap_frames); });
    if (visible_frame_end_locked() == 0)
        throw_terminal_locked();
    return claim_read_locked(frames_per_read, overlap_frames);
}

PipeCoreFrameWindow PipeCore::claim_read_locked(size_t frames_per_read,
                                                size_t overlap_frames)
{
    const size_t start = read_start_locked(overlap_frames);
    const size_t end = read_end_locked(start, frames_per_read);
    const size_t frame_count = end - start;
    const size_t actual_overlap = read_reserved_frame_end_ - start;
    const size_t new_frame_count = frame_count - actual_overlap;
    if (new_frame_count == 0)
        throw_terminal_locked();
    PipeCoreFrameWindow window;
    window.token_id = next_token_id_++;
    window.start_frame_index = start;
    window.frame_count = frame_count;
    window.overlap_frames = actual_overlap;
    window.new_frame_count = new_frame_count;
    window.is_shot_start = start == read_shot_start_frame_index_;
    window.is_shot_end = next_shot_end_frame_locked() != 0 &&
                         end == next_shot_end_frame_locked();
    window.slots.reserve(new_frame_count);
    for (size_t i = 0; i < new_frame_count; ++i)
        window.slots.push_back(
            frame_index_to_slot(read_reserved_frame_end_ + i));
    pending_reads_.push_back(
        PendingRead{window.token_id, start, new_frame_count, false});
    read_reserved_frame_end_ += new_frame_count;
    if (window.is_shot_end)
        read_shot_start_frame_index_ = end;
    return window;
}

void PipeCore::release_read_window(size_t token_id)
{
    std::lock_guard<std::mutex> lk(mutex_);
    auto& token = find_read_locked(token_id);
    token.released = true;
    flush_released_reads_locked();
    cv_.notify_all();
}
} // namespace takt
