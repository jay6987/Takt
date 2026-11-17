// Implements synchronized slot reservation, window release and pipe closure.
#include "takt/pipeline/pipe_core.h"

#include <algorithm>
#include <utility>

namespace takt
{
PipeCore::PipeCore(std::string name,
                   size_t slot_capacity,
                   size_t retained_overlap_frames,
                   size_t max_frames_per_write)
    : name_(std::move(name)),
      slot_capacity_(slot_capacity),
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
        throw PipeUnterminatedShotError("pipe '" + name_ +
                                        "' closed with an unterminated Shot");
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

} // namespace takt
