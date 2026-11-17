// Defines a typed pipe token and its ownership behavior.
#pragma once

#include "takt/pipeline/pipe_declaration.h"

namespace takt
{
/// Owns a read window until its slots are returned to the pipe.
template <typename T> class Pipe<T>::ReadToken
{
  public:
    // Creates an empty token with no active read window.
    ReadToken() = default;

    // Transfers window ownership and disarms the source token.
    ReadToken(ReadToken&& rhs) noexcept
        : owner_(rhs.owner_),
          token_id_(rhs.token_id_),
          is_shot_start_(rhs.is_shot_start_),
          is_shot_end_(rhs.is_shot_end_),
          slots_(std::move(rhs.slots_)),
          overlap_frames_(rhs.overlap_frames_),
          frame_count_(rhs.frame_count_)
    {
        rhs.owner_ = nullptr;
    }

    // Releases the previous window before taking the incoming window.
    ReadToken& operator=(ReadToken&& rhs) noexcept;

    // Read windows have a single release owner.
    ReadToken(const ReadToken&) = delete;
    // Prevents copying read-window release responsibility.
    ReadToken& operator=(const ReadToken&) = delete;

    // Releases the active read window on scope exit.
    ~ReadToken()
    {
        release();
    }

    // Returns frames exposed by this window, including retained overlap.
    size_t frame_count() const noexcept
    {
        return frame_count_;
    }
    // Returns how many frames overlap the preceding read window.
    size_t overlap_frames() const noexcept
    {
        return overlap_frames_;
    }
    // Reports whether this read window begins a shot.
    bool is_shot_start() const noexcept
    {
        return is_shot_start_;
    }
    // Reports whether this read window ends a shot.
    bool is_shot_end() const noexcept
    {
        return is_shot_end_;
    }

    // Returns mutable window storage with bounds checking.
    T& mutable_value(size_t index)
    {
        if (index >= frame_count_)
        {
            throw std::out_of_range("read frame index out of range");
        }
        return owner_->frames_[slots_[index]];
    }

    // Returns a read-window frame with bounds checking.
    const T& value(size_t index) const
    {
        if (index >= frame_count_)
        {
            throw std::out_of_range("read frame index out of range");
        }
        return owner_->frames_[slots_[index]];
    }

    // Returns the sole mutable frame, rejecting multi-frame windows.
    T& mutable_value()
    {
        if (frame_count_ != 1)
        {
            throw std::logic_error("read token contains multiple frames; "
                                   "use mutable_value(index)");
        }
        return mutable_value(0);
    }

    // Returns the sole frame, rejecting multi-frame windows.
    const T& value() const
    {
        if (frame_count_ != 1)
        {
            throw std::logic_error(
                "read token contains multiple frames; use value(index)");
        }
        return value(0);
    }

  private:
    friend class Pipe<T>;
    // Binds a core read window to typed slots and shot metadata.
    ReadToken(Pipe<T>* owner,
              size_t token_id,
              bool is_shot_start,
              bool is_shot_end,
              std::vector<size_t>&& slots,
              size_t overlap_frames,
              size_t frame_count)
        : owner_(owner),
          token_id_(token_id),
          is_shot_start_(is_shot_start),
          is_shot_end_(is_shot_end),
          slots_(std::move(slots)),
          overlap_frames_(overlap_frames),
          frame_count_(frame_count)
    {
    }

    // Returns the core window exactly once and disarms this token.
    void release()
    {
        if (!owner_)
        {
            return;
        }
        owner_->release_read_window(token_id_);
        owner_ = nullptr;
    }

    Pipe<T>* owner_ = nullptr;
    size_t token_id_ = 0;
    bool is_shot_start_ = false;
    bool is_shot_end_ = false;
    std::vector<size_t> slots_;
    size_t overlap_frames_ = 0;
    size_t frame_count_ = 0;
};
} // namespace takt

#include "takt/pipeline/pipe_read_token_move.h"
