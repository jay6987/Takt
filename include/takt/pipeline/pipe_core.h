#pragma once

#include <condition_variable>
#include <cstddef>
#include <deque>
#include <mutex>
#include <stdexcept>
#include <string>
#include <vector>

namespace takt
{
struct PipeClosedAndEmptySignal : public std::runtime_error
{
    PipeClosedAndEmptySignal() : std::runtime_error("pipe is closed and empty") {}
};

struct PipeUnterminatedShotError : public std::runtime_error
{
    explicit PipeUnterminatedShotError(const std::string& message)
        : std::runtime_error(message)
    {
    }
};

struct PipeFaultedError : public std::runtime_error
{
    explicit PipeFaultedError(const std::string& message) : std::runtime_error(message)
    {
    }
};

struct PipeWriteAbandonedError : public PipeFaultedError
{
    explicit PipeWriteAbandonedError(const std::string& message)
        : PipeFaultedError(message)
    {
    }
};

struct PipeCoreWriteReservation
{
    size_t token_id = 0;
    size_t start_frame_index = 0;
    bool is_shot_start = false;
    bool is_shot_end = false;
    std::vector<size_t> slots;
};

struct PipeCoreFrameWindow
{
    size_t token_id = 0;
    size_t start_frame_index = 0;
    bool is_shot_start = false;
    bool is_shot_end = false;
    std::vector<size_t> slots;
    size_t frame_count = 0;
    size_t overlap_frames = 0;
    size_t new_frame_count = 0;
};

class PipeCore
{
  public:
    PipeCore(std::string name, size_t slot_capacity, size_t retained_overlap_frames,
             size_t max_frames_per_write);

    PipeCore(const PipeCore&) = delete;
    PipeCore& operator=(const PipeCore&) = delete;

    const std::string& name() const noexcept;
    size_t slot_capacity() const noexcept;

    size_t queued_frame_count() const noexcept;
    bool closed() const noexcept;
    bool faulted() const noexcept;
    std::string fault_message() const;
    void close();
    void close_with_error(std::string message);

    PipeCoreWriteReservation reserve_write(size_t frame_count, bool is_shot_end);
    void release_write_reservation(size_t token_id, bool published);

    PipeCoreFrameWindow acquire_read_window(size_t frames_per_read, size_t overlap_frames);
    void release_read_window(size_t token_id);

  private:
    struct PendingWrite
    {
        size_t token_id = 0;
        size_t start_frame_index = 0;
        size_t frame_count = 0;
        bool is_shot_start = false;
        bool is_shot_end = false;
        bool released = false;
        bool published = false;
    };

    struct PendingRead
    {
        size_t token_id = 0;
        size_t start_frame_index = 0;
        size_t new_frame_count = 0;
        bool released = false;
    };

    size_t frame_index_to_slot(size_t frame_index) const noexcept;
    size_t reusable_frame_start_locked() const noexcept;
    void flush_released_writes_locked();
    void flush_released_reads_locked();
    [[noreturn]] void throw_terminal_locked() const;
    size_t next_shot_end_frame_locked() const;
    size_t visible_frame_end_locked() const;
    PendingWrite& find_write_locked(size_t token_id);
    PendingRead& find_read_locked(size_t token_id);

    std::string name_;
    const size_t slot_capacity_;
    const size_t retained_overlap_frames_;
    const size_t max_frames_per_write_;

    mutable std::mutex mutex_;
    std::condition_variable cv_;

    size_t next_token_id_ = 1;
    size_t write_reserved_frame_end_ = 0;
    size_t write_visible_frame_end_ = 0;
    size_t read_reserved_frame_end_ = 0;
    size_t read_released_frame_end_ = 0;
    size_t read_shot_start_frame_index_ = 0;

    bool next_write_is_shot_start_ = true;
    std::deque<size_t> committed_shot_ends_;

    std::deque<PendingWrite> pending_writes_;
    std::deque<PendingRead> pending_reads_;
    bool write_failure_ = false;
    bool publication_blocked_ = false;
    bool unterminated_close_reported_ = false;
    std::string fault_message_;
    bool closed_ = false;
};
} // namespace takt
