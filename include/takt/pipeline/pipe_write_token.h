// Defines a typed pipe token and its ownership behavior.
#pragma once

#include "takt/pipeline/pipe_write_reservation.h"

namespace takt
{
/// Exclusive producer token that publishes its reservation on release.
template <typename T> class Pipe<T>::WriteToken
{
  public:
    // Creates an empty token that owns no frame reservation.
    WriteToken() = default;

    // Wraps an existing reservation without copying frame payloads.
    explicit WriteToken(WriteReservation&& reservation)
        : reservation_(std::move(reservation))
    {
    }

    // Transfers exclusive reservation ownership from another token.
    WriteToken(WriteToken&& rhs) noexcept
        : reservation_(std::move(rhs.reservation_))
    {
    }

    // Releases the previous reservation and takes the incoming one.
    WriteToken& operator=(WriteToken&& rhs) noexcept
    {
        if (this == &rhs)
        {
            return *this;
        }
        reservation_ = std::move(rhs.reservation_);
        return *this;
    }

    // Reservations cannot have multiple owning write tokens.
    WriteToken(const WriteToken&) = delete;
    // Prevents copying exclusive reservation ownership.
    WriteToken& operator=(const WriteToken&) = delete;

    // Lets the reservation publish or cancel on scope exit.
    ~WriteToken() = default;

    // Returns the number of reserved frame slots.
    size_t frame_count() const noexcept
    {
        return reservation_.frame_count();
    }

    // Returns the writable frame, rejecting multi-frame tokens.
    T& value()
    {
        if (frame_count() != 1)
        {
            throw std::logic_error(
                "write token contains multiple frames; use value(index)");
        }
        return reservation_.value(0);
    }
    // Returns the sole frame for inspection, rejecting multi-frame tokens.
    const T& value() const
    {
        if (frame_count() != 1)
        {
            throw std::logic_error(
                "write token contains multiple frames; use value(index)");
        }
        return reservation_.value(0);
    }
    // Returns a writable reserved frame with bounds checking.
    T& value(size_t index)
    {
        return reservation_.value(index);
    }
    // Returns a reserved frame for inspection with bounds checking.
    const T& value(size_t index) const
    {
        return reservation_.value(index);
    }

    // Marks frames for commit when reservation ownership is released.
    void publish() noexcept
    {
        reservation_.publish();
    }
    // Reports whether commit has been requested.
    bool published() const noexcept
    {
        return reservation_.published();
    }
    // Reports whether this batch begins a shot.
    bool is_shot_start() const noexcept
    {
        return reservation_.is_shot_start();
    }
    // Reports whether this batch ends a shot.
    bool is_shot_end() const noexcept
    {
        return reservation_.is_shot_end();
    }

  private:
    WriteReservation reservation_;
};

} // namespace takt
