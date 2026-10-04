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

struct PipeCoreWriteBatch
{
    size_t token_id = 0;
    size_t start_pos = 0;
    bool is_segment_start = false;
    bool is_segment_end = false;
    std::vector<size_t> slots;
};

struct PipeCoreReadBatch
{
    size_t token_id = 0;
    size_t start_pos = 0;
    bool is_segment_start = false;
    bool is_segment_end = false;
    std::vector<size_t> slots;
    size_t read_size = 0;
    size_t overlap_size = 0;
    size_t stride = 0;
};

class PipeCore
{
  public:
    PipeCore(std::string name, size_t capacity);

    PipeCore(const PipeCore&) = delete;
    PipeCore& operator=(const PipeCore&) = delete;

    const std::string& name() const noexcept;
    size_t capacity() const noexcept;

    size_t size() const noexcept;
    bool closed() const noexcept;
    bool faulted() const noexcept;
    const std::string& fault_message() const noexcept;
    void close();

    PipeCoreWriteBatch acquire_write_batch(size_t count, bool is_segment_end);
    void release_write_batch(size_t token_id, bool published);

    PipeCoreReadBatch acquire_read_batch(size_t read_size, size_t overlap_size,
                                         bool has_previous_read,
                                         size_t previous_read_size);
    void release_read_batch(size_t token_id);

  private:
    struct PendingWrite
    {
        size_t token_id = 0;
        size_t start_pos = 0;
        size_t size = 0;
        bool is_segment_start = false;
        bool is_segment_end = false;
        bool released = false;
        bool published = false;
    };

    struct PendingRead
    {
        size_t token_id = 0;
        size_t start_pos = 0;
        size_t size = 0;
        bool released = false;
    };

    size_t position_to_slot(size_t absolute_pos) const noexcept;
    void flush_released_writes_locked();
    void flush_released_reads_locked();
    PendingWrite& find_write_locked(size_t token_id);
    PendingRead& find_read_locked(size_t token_id);

    std::string name_;
    const size_t capacity_;

    mutable std::mutex mutex_;
    std::condition_variable cv_;

    size_t next_token_id_ = 1;
    size_t write_reserve_pos_ = 0;
    size_t write_visible_pos_ = 0;
    size_t read_reserve_pos_ = 0;
    size_t read_release_pos_ = 0;
    size_t read_segment_start_pos_ = 0;

    bool next_write_is_segment_start_ = true;
    std::deque<size_t> committed_segment_ends_;

    std::deque<PendingWrite> pending_writes_;
    std::deque<PendingRead> pending_reads_;
    bool write_failure_ = false;
    std::string fault_message_;
    bool closed_ = false;
};
} // namespace takt
