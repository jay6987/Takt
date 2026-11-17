// Defines a typed pipe token and its ownership behavior.
#pragma once

#include "takt/pipeline/pipe_declaration.h"

namespace takt
{
/// Owns reserved write slots until publication or abandonment.
template <typename T> class Pipe<T>::WriteReservation
{
  public:
    // Creates an empty reservation with no owned slots.
    WriteReservation() = default;

    // Transfers slots and leaves the source unable to release them.
    WriteReservation(WriteReservation&& rhs) noexcept
        : owner_(rhs.owner_),
          token_id_(rhs.token_id_),
          is_shot_start_(rhs.is_shot_start_),
          is_shot_end_(rhs.is_shot_end_),
          slots_(std::move(rhs.slots_)),
          published_(rhs.published_)
    {
        rhs.owner_ = nullptr;
    }

    // Releases held slots before taking another reservation's ownership.
    WriteReservation& operator=(WriteReservation&& rhs) noexcept
    {
        if (this == &rhs)
        {
            return *this;
        }
        release();
        owner_ = rhs.owner_;
        token_id_ = rhs.token_id_;
        is_shot_start_ = rhs.is_shot_start_;
        is_shot_end_ = rhs.is_shot_end_;
        slots_ = std::move(rhs.slots_);
        published_ = rhs.published_;
        rhs.owner_ = nullptr;
        return *this;
    }

    // Slot reservations are exclusively owned.
    WriteReservation(const WriteReservation&) = delete;
    // Prevents copying the release responsibility.
    WriteReservation& operator=(const WriteReservation&) = delete;

    // Commits published frames or cancels unpublished reservations.
    ~WriteReservation()
    {
        release();
    }

    // Returns the number of exclusively held frame slots.
    size_t frame_count() const noexcept
    {
        return slots_.size();
    }

    // Returns mutable slot storage or throws for an invalid index.
    T& value(size_t index)
    {
        if (index >= slots_.size())
        {
            throw std::out_of_range("write frame index out of range");
        }
        return owner_->frames_[slots_[index]];
    }

    // Returns slot storage for inspection or throws for an invalid index.
    const T& value(size_t index) const
    {
        if (index >= slots_.size())
        {
            throw std::out_of_range("write frame index out of range");
        }
        return owner_->frames_[slots_[index]];
    }

    // Requests commit; visibility changes only when the slots are released.
    void publish() noexcept
    {
        published_ = true;
    }
    // Reports the requested commit state.
    bool published() const noexcept
    {
        return published_;
    }
    // Reports the core's start-of-shot marker for this reservation.
    bool is_shot_start() const noexcept
    {
        return is_shot_start_;
    }
    // Reports the core's end-of-shot marker for this reservation.
    bool is_shot_end() const noexcept
    {
        return is_shot_end_;
    }

  private:
    friend class Pipe<T>;
    // Binds core reservation metadata to its owning typed frame storage.
    WriteReservation(Pipe<T>* owner,
                     size_t token_id,
                     bool is_shot_start,
                     bool is_shot_end,
                     std::vector<size_t>&& slots)
        : owner_(owner),
          token_id_(token_id),
          is_shot_start_(is_shot_start),
          is_shot_end_(is_shot_end),
          slots_(std::move(slots))
    {
    }

    // Records committed frames, then returns reserved slots exactly once.
    void release()
    {
        if (!owner_)
        {
            return;
        }
        if (published_)
        {
            recordreplay::detail::RecordReplayRegistry::instance()
                .try_record_slots(*owner_, slots_, owner_->frames_);
        }
        owner_->release_write_reservation(token_id_, published_);
        owner_ = nullptr;
    }

    Pipe<T>* owner_ = nullptr;
    size_t token_id_ = 0;
    bool is_shot_start_ = false;
    bool is_shot_end_ = false;
    std::vector<size_t> slots_;
    bool published_ = false;
};

} // namespace takt
