// Synchronizes bounded pipe frame bookkeeping.
#include "takt/pipeline/pipe_core.h"

#include <algorithm>
#include <utility>

namespace takt
{
size_t PipeCore::frame_index_to_slot(size_t frame_index) const noexcept
{
    return frame_index % slot_capacity_;
}

size_t PipeCore::reusable_frame_start_locked() const noexcept
{
    const size_t overlap_start =
        std::max(read_shot_start_frame_index_,
                 read_reserved_frame_end_ > retained_overlap_frames_
                     ? read_reserved_frame_end_ - retained_overlap_frames_
                     : 0);
    size_t reusable_pos = overlap_start;
    for (const auto& token : pending_reads_)
    {
        if (!token.released)
        {
            reusable_pos = std::min(reusable_pos, token.start_frame_index);
        }
    }
    return reusable_pos;
}

void PipeCore::flush_released_writes_locked()
{
    while (!pending_writes_.empty() && pending_writes_.front().released)
    {
        const auto token = pending_writes_.front();
        pending_writes_.pop_front();

        if (publication_blocked_)
        {
            continue;
        }
        if (token.published)
        {
            write_visible_frame_end_ += token.frame_count;
            if (token.is_shot_end)
            {
                committed_shot_ends_.push_back(write_visible_frame_end_);
            }
        }
        else
        {
            publication_blocked_ = true;
            closed_ = true;
            if (!write_failure_)
            {
                write_failure_ = true;
                fault_message_ = "pipe '" + name_ +
                                 "' faulted because write token starting at "
                                 "logical position " +
                                 std::to_string(token.start_frame_index) +
                                 " with frame count " +
                                 std::to_string(token.frame_count) +
                                 " was released without publish";
            }
        }
    }
}

} // namespace takt
