// Synchronizes bounded pipe frame bookkeeping.
#include "takt/pipeline/pipe_core.h"

#include <algorithm>
#include <utility>

namespace takt
{
void PipeCore::throw_terminal_locked() const
{
    if (write_failure_)
    {
        throw PipeFaultedError(fault_message_);
    }
    throw PipeClosedAndEmptySignal();
}

size_t PipeCore::next_shot_end_frame_locked() const
{
    const auto end =
        std::upper_bound(committed_shot_ends_.begin(),
                         committed_shot_ends_.end(), read_reserved_frame_end_);
    return end == committed_shot_ends_.end() ? 0 : *end;
}

size_t PipeCore::visible_frame_end_locked() const
{
    const size_t shot_end = next_shot_end_frame_locked();
    if (shot_end != 0)
    {
        return shot_end;
    }
    if (read_reserved_frame_end_ < write_visible_frame_end_)
    {
        return write_visible_frame_end_;
    }
    return 0;
}

void PipeCore::flush_released_reads_locked()
{
    while (!pending_reads_.empty() && pending_reads_.front().released)
    {
        read_released_frame_end_ += pending_reads_.front().new_frame_count;
        pending_reads_.pop_front();

        while (!committed_shot_ends_.empty() &&
               committed_shot_ends_.front() <= read_released_frame_end_)
        {
            committed_shot_ends_.pop_front();
        }
    }
}

PipeCore::PendingWrite& PipeCore::find_write_locked(size_t token_id)
{
    for (auto& token : pending_writes_)
    {
        if (token.token_id == token_id)
        {
            return token;
        }
    }
    throw std::logic_error("write token id not found");
}

PipeCore::PendingRead& PipeCore::find_read_locked(size_t token_id)
{
    for (auto& token : pending_reads_)
    {
        if (token.token_id == token_id)
        {
            return token;
        }
    }
    throw std::logic_error("read token id not found");
}
} // namespace takt
