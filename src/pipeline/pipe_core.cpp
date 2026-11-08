#include "takt/pipeline/pipe_core.h"

#include <algorithm>
#include <utility>

namespace takt
{
PipeCore::PipeCore(std::string name, size_t slot_capacity, size_t retained_overlap_frames,
                   size_t max_frames_per_write)
    : name_(std::move(name)), slot_capacity_(slot_capacity),
      retained_overlap_frames_(retained_overlap_frames),
      max_frames_per_write_(max_frames_per_write)
{
    if (slot_capacity_ == 0)
    {
        throw std::invalid_argument("pipe slot capacity cannot be zero");
    }
}

const std::string& PipeCore::name() const noexcept
{
    return name_;
}

size_t PipeCore::slot_capacity() const noexcept
{
    return slot_capacity_;
}

size_t PipeCore::queued_frame_count() const noexcept
{
    std::lock_guard<std::mutex> lk(mutex_);
    return write_visible_frame_end_ - read_reserved_frame_end_;
}

bool PipeCore::closed() const noexcept
{
    std::lock_guard<std::mutex> lk(mutex_);
    return closed_;
}

bool PipeCore::faulted() const noexcept
{
    std::lock_guard<std::mutex> lk(mutex_);
    return write_failure_;
}

std::string PipeCore::fault_message() const
{
    std::lock_guard<std::mutex> lk(mutex_);
    return fault_message_;
}

void PipeCore::close()
{
    bool report_unterminated = false;
    {
        std::lock_guard<std::mutex> lk(mutex_);
        closed_ = true;
        if (!next_write_is_shot_start_ && !unterminated_close_reported_)
        {
            unterminated_close_reported_ = true;
            report_unterminated = true;
        }
        cv_.notify_all();
    }
    if (report_unterminated)
    {
        throw PipeUnterminatedShotError(
            "pipe '" + name_ + "' closed with an unterminated Shot");
    }
}

void PipeCore::close_with_error(std::string message)
{
    if (message.empty())
    {
        throw std::invalid_argument("pipe error message cannot be empty");
    }
    std::lock_guard<std::mutex> lk(mutex_);
    if (!write_failure_)
    {
        fault_message_ = std::move(message);
        write_failure_ = true;
    }
    closed_ = true;
    cv_.notify_all();
}

PipeCoreWriteReservation PipeCore::reserve_write(size_t frame_count, bool is_shot_end)
{
    if (frame_count == 0)
    {
        throw std::invalid_argument("write frame count cannot be zero");
    }
    if (frame_count > slot_capacity_)
    {
        throw std::invalid_argument("write frame count cannot exceed slot capacity");
    }
    if (frame_count > max_frames_per_write_)
    {
        throw std::invalid_argument(
            "write frame count cannot exceed configured frames_per_write");
    }

    std::unique_lock<std::mutex> lk(mutex_);
    cv_.wait(lk,
             [this, frame_count]
             {
                 return closed_ || write_failure_ ||
                        (write_reserved_frame_end_ + frame_count <=
                         reusable_frame_start_locked() + slot_capacity_);
             });

    if (write_failure_)
    {
        throw PipeFaultedError(fault_message_);
    }
    if (closed_)
    {
        throw PipeClosedAndEmptySignal();
    }

    PipeCoreWriteReservation reservation;
    reservation.token_id = next_token_id_++;
    reservation.start_frame_index = write_reserved_frame_end_;
    reservation.is_shot_start = next_write_is_shot_start_;
    reservation.is_shot_end = is_shot_end;
    reservation.slots.reserve(frame_count);
    for (size_t i = 0; i < frame_count; ++i)
    {
        reservation.slots.push_back(frame_index_to_slot(write_reserved_frame_end_ + i));
    }

    pending_writes_.push_back(PendingWrite{reservation.token_id,
                                           reservation.start_frame_index, frame_count,
                                           reservation.is_shot_start,
                                           reservation.is_shot_end,
                                           false, false});
    write_reserved_frame_end_ += frame_count;
    next_write_is_shot_start_ = is_shot_end;

    return reservation;
}

void PipeCore::release_write_reservation(size_t token_id, bool published)
{
    std::lock_guard<std::mutex> lk(mutex_);
    auto& token = find_write_locked(token_id);
    token.released = true;
    token.published = published;
    flush_released_writes_locked();
    cv_.notify_all();
}

PipeCoreFrameWindow PipeCore::acquire_read_window(size_t frames_per_read,
                                                  size_t overlap_frames)
{
    if (frames_per_read == 0)
    {
        throw std::invalid_argument("read frame count cannot be zero");
    }
    if (overlap_frames >= frames_per_read)
    {
        throw std::invalid_argument(
            "overlap frame count must be smaller than read frame count");
    }
    if (overlap_frames > retained_overlap_frames_)
    {
        throw std::invalid_argument("requested overlap exceeds configured retention");
    }

    const size_t required_new_frame_count = frames_per_read - overlap_frames;

    auto calc_read_end_frame_index = [this](size_t start_frame_index, size_t wanted_size) -> size_t
    {
        const size_t read_end_frame_index = visible_frame_end_locked();
        if (read_end_frame_index == 0)
        {
            return start_frame_index;
        }
        return std::min(start_frame_index + wanted_size, read_end_frame_index);
    };

    std::unique_lock<std::mutex> lk(mutex_);
    const size_t available_overlap =
        std::min(overlap_frames, read_reserved_frame_end_ - read_shot_start_frame_index_);
    if (frames_per_read - available_overlap > slot_capacity_)
    {
        throw std::invalid_argument("new read slots cannot exceed pipe capacity");
    }
    cv_.wait(
        lk,
        [this, frames_per_read, overlap_frames, required_new_frame_count,
         &calc_read_end_frame_index]
        {
            if (closed_ && pending_writes_.empty())
            {
                return true;
            }
            if (visible_frame_end_locked() == 0)
            {
                return false;
            }

            const size_t read_start_frame_index =
                std::max(read_shot_start_frame_index_, read_reserved_frame_end_ > overlap_frames
                                                      ? read_reserved_frame_end_ - overlap_frames
                                                      : 0);
            const size_t read_end_frame_index = calc_read_end_frame_index(read_start_frame_index, frames_per_read);
            const size_t frame_count = read_end_frame_index - read_start_frame_index;
            const size_t actual_overlap = read_reserved_frame_end_ - read_start_frame_index;
            const size_t new_frame_count = frame_count - actual_overlap;
            const bool hit_shot_end =
                next_shot_end_frame_locked() != 0 &&
                read_end_frame_index == next_shot_end_frame_locked();

            // Return a short final window at ShotEnd instead of waiting for a
            // full window that cannot be completed within this Shot.
            return new_frame_count >= required_new_frame_count ||
                   (hit_shot_end && new_frame_count > 0) ||
                   (closed_ && new_frame_count > 0);
        });

    if (visible_frame_end_locked() == 0)
    {
        throw_terminal_locked();
    }

    const size_t read_start_frame_index = std::max(
        read_shot_start_frame_index_,
        read_reserved_frame_end_ > overlap_frames ? read_reserved_frame_end_ - overlap_frames : 0);
    const size_t read_end_frame_index = calc_read_end_frame_index(read_start_frame_index, frames_per_read);
    const size_t frame_count = read_end_frame_index - read_start_frame_index;
    const size_t actual_overlap = read_reserved_frame_end_ - read_start_frame_index;
    const size_t new_frame_count = frame_count - actual_overlap;

    if (new_frame_count == 0)
    {
        throw_terminal_locked();
    }

    PipeCoreFrameWindow window;
    window.token_id = next_token_id_++;
    window.start_frame_index = read_start_frame_index;
    window.frame_count = frame_count;
    window.overlap_frames = actual_overlap;
    window.new_frame_count = new_frame_count;
    window.is_shot_start = (read_start_frame_index == read_shot_start_frame_index_);
    window.is_shot_end =
        next_shot_end_frame_locked() != 0 &&
        read_end_frame_index == next_shot_end_frame_locked();
    window.slots.reserve(new_frame_count);

    for (size_t i = 0; i < new_frame_count; ++i)
    {
        window.slots.push_back(frame_index_to_slot(read_reserved_frame_end_ + i));
    }

    pending_reads_.push_back(
        PendingRead{window.token_id, read_start_frame_index, new_frame_count, false});
    read_reserved_frame_end_ += new_frame_count;

    if (window.is_shot_end)
    {
        read_shot_start_frame_index_ = read_end_frame_index;
    }

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

size_t PipeCore::frame_index_to_slot(size_t frame_index) const noexcept
{
    return frame_index % slot_capacity_;
}

size_t PipeCore::reusable_frame_start_locked() const noexcept
{
    const size_t overlap_start = std::max(
        read_shot_start_frame_index_,
        read_reserved_frame_end_ > retained_overlap_frames_ ? read_reserved_frame_end_ - retained_overlap_frames_ : 0);
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
                fault_message_ =
                    "pipe '" + name_ +
                    "' faulted because write token starting at logical position " +
                    std::to_string(token.start_frame_index) + " with frame count " +
                    std::to_string(token.frame_count) + " was released without publish";
            }
        }
    }
}

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
    const auto end = std::upper_bound(committed_shot_ends_.begin(),
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
