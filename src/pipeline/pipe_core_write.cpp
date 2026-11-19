// Synchronizes bounded pipe frame bookkeeping.
#include "takt/pipeline/pipe_core.h"

#include <algorithm>
#include <utility>

#include "statistics_timer.h"

namespace takt
{
void PipeCore::validate_write(size_t frame_count) const
{
    if (frame_count == 0)
    {
        throw std::invalid_argument("write frame count cannot be zero");
    }
    if (frame_count > slot_capacity_)
    {
        throw std::invalid_argument(
            "write frame count cannot exceed slot capacity");
    }
    if (frame_count > max_frames_per_write_)
    {
        throw std::invalid_argument(
            "write frame count cannot exceed configured frames_per_write");
    }
}

PipeCoreWriteReservation PipeCore::reserve_write(size_t frame_count,
                                                 bool is_shot_end)
{
    validate_write(frame_count);
    std::unique_lock<std::mutex> lk(mutex_);
    const auto ready = [this, frame_count]
    {
        return closed_ || write_failure_ ||
               (write_reserved_frame_end_ + frame_count <=
                reusable_frame_start_locked() + slot_capacity_);
    };
    auto* wait = statistics_ && !ready() ? &statistics_->write_wait : nullptr;
    {
        const StatisticsTimer timer(wait ? &wait->time : nullptr);
        cv_.wait(lk, ready);
    }
    if (wait)
        ++wait->count;

    if (write_failure_)
    {
        throw PipeFaultedError(fault_message_);
    }
    if (closed_)
    {
        throw PipeClosedAndEmptySignal();
    }

    return claim_write_locked(frame_count, is_shot_end);
}

PipeCoreWriteReservation PipeCore::claim_write_locked(size_t frame_count,
                                                      bool is_shot_end)
{
    PipeCoreWriteReservation reservation;
    reservation.token_id = next_token_id_++;
    reservation.start_frame_index = write_reserved_frame_end_;
    reservation.is_shot_start = next_write_is_shot_start_;
    reservation.is_shot_end = is_shot_end;
    reservation.slots.reserve(frame_count);
    for (size_t i = 0; i < frame_count; ++i)
    {
        reservation.slots.push_back(
            frame_index_to_slot(write_reserved_frame_end_ + i));
    }

    pending_writes_.push_back(PendingWrite{
        reservation.token_id, reservation.start_frame_index, frame_count,
        reservation.is_shot_start, reservation.is_shot_end, false, false});
    write_reserved_frame_end_ += frame_count;
    if (statistics_)
        statistics_->peak_unreusable_slots = std::max(
            statistics_->peak_unreusable_slots,
            write_reserved_frame_end_ - reusable_frame_start_locked());
    next_write_is_shot_start_ = is_shot_end;

    return reservation;
}

void PipeCore::release_write_reservation(size_t token_id,
                                         bool published)
{
    std::lock_guard<std::mutex> lk(mutex_);
    auto& token = find_write_locked(token_id);
    token.released = true;
    token.published = published;
    flush_released_writes_locked();
    cv_.notify_all();
}

} // namespace takt
