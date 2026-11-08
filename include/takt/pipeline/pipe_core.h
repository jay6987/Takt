// Manages bounded frame slots and synchronized read/write token lifetimes.
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
    // Signals normal completion after a closed pipe has been drained.
    PipeClosedAndEmptySignal() : std::runtime_error("pipe is closed and empty")
    {
    }
};

struct PipeUnterminatedShotError : public std::runtime_error
{
    // Carries diagnostic context for closure without a shot-end marker.
    explicit PipeUnterminatedShotError(const std::string& message)
        : std::runtime_error(message)
    {
    }
};

struct PipeFaultedError : public std::runtime_error
{
    // Carries the pipe's terminal failure message.
    explicit PipeFaultedError(const std::string& message)
        : std::runtime_error(message)
    {
    }
};

struct PipeWriteAbandonedError : public PipeFaultedError
{
    // Identifies a cancelled write that blocks ordered publication.
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
    // Creates bounded synchronized slot bookkeeping for configured batching.
    PipeCore(std::string name,
             size_t slot_capacity,
             size_t retained_overlap_frames,
             size_t max_frames_per_write);

    // Synchronization and token identity cannot be copied.
    PipeCore(const PipeCore&) = delete;
    // Prevents replacement of bookkeeping referenced by active tokens.
    PipeCore& operator=(const PipeCore&) = delete;

    // Returns the diagnostic pipe name.
    const std::string& name() const noexcept;
    // Returns the immutable number of frame slots.
    size_t slot_capacity() const noexcept;

    // Returns frames visible to readers under synchronization.
    size_t queued_frame_count() const noexcept;
    // Reports terminal closure under synchronization.
    bool closed() const noexcept;
    // Reports a retained terminal fault under synchronization.
    bool faulted() const noexcept;
    // Returns the retained terminal fault message.
    std::string fault_message() const;
    // Closes the core and wakes blocked readers and writers.
    void close();
    // Closes with a nonempty message while retaining the first fault.
    void close_with_error(std::string message);

    // Reserves a valid batch in producer order, waiting for reusable slots.
    PipeCoreWriteReservation reserve_write(size_t frame_count,
                                           bool is_shot_end);
    // Commits a released batch in order or marks abandoned publication.
    void release_write_reservation(size_t token_id,
                                   bool published);

    // Waits for a shot-bounded read window including configured overlap.
    PipeCoreFrameWindow acquire_read_window(size_t frames_per_read,
                                            size_t overlap_frames);
    // Releases a read window and reclaims slots in read reservation order.
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

    // Maps an absolute frame index into bounded circular storage.
    size_t frame_index_to_slot(size_t frame_index) const noexcept;
    // Finds the first frame no longer needed for retained overlap.
    size_t reusable_frame_start_locked() const noexcept;
    // Publishes consecutive released writes while the mutex is held.
    void flush_released_writes_locked();
    // Reclaims consecutive released reads while the mutex is held.
    void flush_released_reads_locked();
    // Throws the appropriate terminal fault or normal completion signal.
    [[noreturn]] void throw_terminal_locked() const;
    // Finds the next committed shot boundary after the read position.
    size_t next_shot_end_frame_locked() const;
    // Returns the publication frontier, accounting for blocked writes.
    size_t visible_frame_end_locked() const;
    // Looks up a pending write or rejects an unknown token.
    PendingWrite& find_write_locked(size_t token_id);
    // Looks up a pending read or rejects an unknown token.
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
