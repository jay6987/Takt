#include "takt/pipeline/pipe_core.h"

#include <algorithm>
#include <utility>

namespace takt
{
PipeCore::PipeCore(std::string name, size_t capacity)
    : name_(std::move(name)), capacity_(capacity)
{
    if (capacity_ == 0)
    {
        throw std::invalid_argument("pipe capacity cannot be zero");
    }
}

const std::string& PipeCore::name() const noexcept
{
    return name_;
}

size_t PipeCore::capacity() const noexcept
{
    return capacity_;
}

size_t PipeCore::size() const noexcept
{
    std::lock_guard<std::mutex> lk(mutex_);
    return write_visible_pos_ - read_reserve_pos_;
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

const std::string& PipeCore::fault_message() const noexcept
{
    return fault_message_;
}

void PipeCore::close()
{
    std::lock_guard<std::mutex> lk(mutex_);
    closed_ = true;
    cv_.notify_all();
}

PipeCoreWriteBatch PipeCore::acquire_write_batch(size_t count, bool is_segment_end)
{
    if (count == 0)
    {
        throw std::invalid_argument("write batch count cannot be zero");
    }
    if (count > capacity_)
    {
        throw std::invalid_argument("write batch count cannot exceed pipe capacity");
    }

    std::unique_lock<std::mutex> lk(mutex_);
    cv_.wait(lk,
             [this, count]
             {
                 return closed_ || write_failure_ ||
                        (write_reserve_pos_ + count <= read_release_pos_ + capacity_);
             });

    if (write_failure_)
    {
        throw PipeFaultedError(fault_message_);
    }
    if (closed_)
    {
        throw PipeClosedAndEmptySignal();
    }

    PipeCoreWriteBatch batch;
    batch.token_id = next_token_id_++;
    batch.start_pos = write_reserve_pos_;
    batch.is_segment_start = next_write_is_segment_start_;
    batch.is_segment_end = is_segment_end;
    batch.slots.reserve(count);
    for (size_t i = 0; i < count; ++i)
    {
        batch.slots.push_back(position_to_slot(write_reserve_pos_ + i));
    }

    pending_writes_.push_back(PendingWrite{batch.token_id, batch.start_pos, count,
                                           batch.is_segment_start, batch.is_segment_end,
                                           false, false});
    write_reserve_pos_ += count;
    next_write_is_segment_start_ = is_segment_end;

    return batch;
}

void PipeCore::release_write_batch(size_t token_id, bool published)
{
    std::lock_guard<std::mutex> lk(mutex_);
    auto& token = find_write_locked(token_id);
    token.released = true;
    token.published = published;
    flush_released_writes_locked();
    cv_.notify_all();
}

PipeCoreReadBatch PipeCore::acquire_read_batch(size_t read_size, size_t overlap_size,
                                               bool has_previous_read,
                                               size_t previous_read_size)
{
    if (read_size == 0)
    {
        throw std::invalid_argument("read size cannot be zero");
    }
    if (read_size > capacity_)
    {
        throw std::invalid_argument("read size cannot exceed pipe capacity");
    }
    if (overlap_size >= read_size)
    {
        throw std::invalid_argument("overlap size must be smaller than read size");
    }
    if (overlap_size > 0 && !has_previous_read)
    {
        throw std::logic_error("overlap read requires at least one previous batch");
    }
    if (overlap_size > 0 && previous_read_size < overlap_size)
    {
        throw std::logic_error("overlap cache is smaller than requested overlap size");
    }

    const size_t stride = read_size - overlap_size;

    auto calc_read_end = [this](size_t start_pos, size_t wanted_size) -> size_t
    {
        if (committed_segment_ends_.empty())
        {
            return start_pos;
        }
        const size_t segment_end = committed_segment_ends_.front();
        return std::min(start_pos + wanted_size, segment_end);
    };

    std::unique_lock<std::mutex> lk(mutex_);
    cv_.wait(
        lk,
        [this, read_size, overlap_size, stride, &calc_read_end]
        {
            if (closed_ && committed_segment_ends_.empty() &&
                read_reserve_pos_ >= write_visible_pos_)
            {
                return true;
            }
            if (committed_segment_ends_.empty())
            {
                return false;
            }

            const size_t read_start =
                std::max(read_segment_start_pos_, read_reserve_pos_ > overlap_size
                                                      ? read_reserve_pos_ - overlap_size
                                                      : 0);
            const size_t read_end = calc_read_end(read_start, read_size);
            const size_t actual_read_size = read_end - read_start;
            const size_t actual_overlap = read_reserve_pos_ - read_start;
            const size_t actual_stride = actual_read_size - actual_overlap;
            const bool hit_segment_end = (!committed_segment_ends_.empty() &&
                                          read_end == committed_segment_ends_.front());

            // If this window reaches the committed segment end, return what is
            // available instead of waiting for the next segment to complete.
            return actual_stride >= stride || (hit_segment_end && actual_stride > 0) ||
                   (closed_ && actual_stride > 0);
        });

    if (committed_segment_ends_.empty())
    {
        throw PipeClosedAndEmptySignal();
    }

    const size_t read_start = std::max(
        read_segment_start_pos_,
        read_reserve_pos_ > overlap_size ? read_reserve_pos_ - overlap_size : 0);
    const size_t read_end = calc_read_end(read_start, read_size);
    const size_t actual_read_size = read_end - read_start;
    const size_t actual_overlap = read_reserve_pos_ - read_start;
    const size_t actual_stride = actual_read_size - actual_overlap;

    if (actual_stride == 0)
    {
        throw PipeClosedAndEmptySignal();
    }

    PipeCoreReadBatch batch;
    batch.token_id = next_token_id_++;
    batch.start_pos = read_start;
    batch.read_size = actual_read_size;
    batch.overlap_size = actual_overlap;
    batch.stride = actual_stride;
    batch.is_segment_start = (read_start == read_segment_start_pos_);
    batch.is_segment_end = (!committed_segment_ends_.empty() &&
                            read_end == committed_segment_ends_.front());
    batch.slots.reserve(actual_stride);

    for (size_t i = 0; i < actual_stride; ++i)
    {
        batch.slots.push_back(position_to_slot(read_reserve_pos_ + i));
    }

    pending_reads_.push_back(
        PendingRead{batch.token_id, read_reserve_pos_, actual_stride, false});
    read_reserve_pos_ += actual_stride;

    if (batch.is_segment_end)
    {
        read_segment_start_pos_ = read_end;
    }

    return batch;
}

void PipeCore::release_read_batch(size_t token_id)
{
    std::lock_guard<std::mutex> lk(mutex_);
    auto& token = find_read_locked(token_id);
    token.released = true;
    flush_released_reads_locked();
    cv_.notify_all();
}

size_t PipeCore::position_to_slot(size_t absolute_pos) const noexcept
{
    return absolute_pos % capacity_;
}

void PipeCore::flush_released_writes_locked()
{
    while (!pending_writes_.empty() && pending_writes_.front().released)
    {
        const auto token = pending_writes_.front();
        pending_writes_.pop_front();

        if (token.published)
        {
            write_visible_pos_ += token.size;
            if (token.is_segment_end)
            {
                committed_segment_ends_.push_back(write_visible_pos_);
            }
        }
        else
        {
            write_failure_ = true;
            closed_ = true;
            fault_message_ =
                "pipe '" + name_ +
                "' faulted because write token starting at logical position " +
                std::to_string(token.start_pos) + " with size " +
                std::to_string(token.size) + " was released without publish";
            return;
        }
    }
}

void PipeCore::flush_released_reads_locked()
{
    while (!pending_reads_.empty() && pending_reads_.front().released)
    {
        read_release_pos_ += pending_reads_.front().size;
        pending_reads_.pop_front();

        while (!committed_segment_ends_.empty() &&
               committed_segment_ends_.front() <= read_release_pos_)
        {
            committed_segment_ends_.pop_front();
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
