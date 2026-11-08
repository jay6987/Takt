// Owns typed frame storage and move-only tokens over a bounded pipe core.
#pragma once

#include <memory>
#include <string>
#include <vector>

#include "takt/pipeline/descriptor.h"
#include "takt/pipeline/pipe_base.h"
#include "takt/pipeline/pipe_core.h"
#include "takt/recordreplay/recordreplay.h"

namespace takt
{
template <typename T> class Pipe : public PipeBase
{
  public:
    class WriteToken;
    class WriteReservation;
    class ReadToken;

    // Creates an unconfigured pipe; frame storage is allocated at initialize.
    explicit Pipe(std::string name) : PipeBase(std::move(name)) {}

    // Sets producer batching and the prototype copied into each frame slot.
    void set_producer(ProducerConfig config,
                      T frame_template)
    {
        require_configuring();
        if (has_producer())
            fail("producer is already configured");
        auto storage = std::make_unique<T>(std::move(frame_template));
        set_producer_config(std::move(config));
        frame_template_ = std::move(storage);
    }

    // Returns the prototype before initialization consumes it.
    const T& frame_template() const
    {
        if (!frame_template_)
            fail("frame template is only available during configuration");
        return *frame_template_;
    }

    // Attaches descriptive metadata while configuration remains mutable.
    void set_descriptor(PipeDescriptor descriptor)
    {
        require_configuring();
        descriptor_ = std::move(descriptor);
    }

    // Rejects incomplete common configuration or a missing frame prototype.
    void validate_configuration() const override
    {
        validate_common();
        if (!frame_template_)
            fail("producer frame template is missing");
    }

    // Allocates bounded slots only after validating all configuration.
    void initialize() override
    {
        validate_configuration();
        const auto slot_count = slot_capacity();
        auto core = std::make_unique<PipeCore>(
            name(), slot_count, consumer_config().overlap_frames,
            producer_config().frames_per_write);
        std::vector<T> frames(slot_count, *frame_template_);
        core_ = std::move(core);
        frames_ = std::move(frames);
        frame_template_.reset();
        initialized_ = true;
    }

    // Pipe identity and token ownership cannot be copied.
    Pipe(const Pipe&) = delete;
    // Prevents replacing storage while tokens may refer to this pipe.
    Pipe& operator=(const Pipe&) = delete;

    // Returns committed frames waiting in the initialized core.
    size_t queued_frame_count() const noexcept
    {
        return core_ ? core_->queued_frame_count() : 0;
    }
    // Reports closure, including cancellation before initialization.
    bool closed() const noexcept override
    {
        return core_ ? core_->closed() : cancelled_;
    }
    // Reports a configured or runtime pipe failure.
    bool faulted() const noexcept override
    {
        return core_ ? core_->faulted() : !configuration_error_.empty();
    }
    // Returns the first configured or runtime failure message.
    std::string fault_message() const
    {
        return core_ ? core_->fault_message() : configuration_error_;
    }
    // Returns optional diagnostic metadata without copying it.
    const PipeDescriptor& descriptor() const noexcept
    {
        return descriptor_;
    }
    // Wakes blocked operations or records pre-initialization cancellation.
    void close() override
    {
        if (core_)
            core_->close();
        else
            cancelled_ = true;
    }

    // Closes with a nonempty failure message, retaining the first failure.
    void close_with_error(std::string message)
    {
        if (message.empty())
            throw std::invalid_argument(
                context("pipe error message cannot be empty"));
        if (core_)
            core_->close_with_error(std::move(message));
        else
        {
            if (configuration_error_.empty())
                configuration_error_ = std::move(message);
            cancelled_ = true;
        }
    }

    // Reserves the producer's configured number of frames, blocking if full.
    WriteToken acquire_write()
    {
        auto token = acquire_write_reservation(
            producer_config().frames_per_write, false);
        return WriteToken(std::move(token));
    }

    // Reserves a possibly partial batch marking the end of a shot.
    WriteToken acquire_shot_end_write(size_t frame_count)
    {
        auto token = acquire_write_reservation(frame_count, true);
        return WriteToken(std::move(token));
    }

    // Acquires a read window and replays bound frames before exposing them.
    ReadToken acquire_read()
    {
        require_initialized();
        const auto& config = consumer_config();
        auto window = core_->acquire_read_window(config.frames_per_read,
                                                 config.overlap_frames);

        try
        {
            recordreplay::detail::RecordReplayRegistry::instance().replay_slots(
                *this, window.slots, frames_);

            std::vector<size_t> slots;
            slots.reserve(window.frame_count);
            for (size_t i = 0; i < window.frame_count; ++i)
            {
                slots.push_back((window.start_frame_index + i) %
                                slot_capacity());
            }

            return ReadToken(this, window.token_id, window.is_shot_start,
                             window.is_shot_end, std::move(slots),
                             window.overlap_frames, window.frame_count);
        }
        catch (...)
        {
            core_->release_read_window(window.token_id);
            throw;
        }
    }

    class WriteToken
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

    class WriteReservation
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

    class ReadToken
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
        ReadToken& operator=(ReadToken&& rhs) noexcept
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
            overlap_frames_ = rhs.overlap_frames_;
            frame_count_ = rhs.frame_count_;
            rhs.owner_ = nullptr;
            return *this;
        }

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

  private:
    // Reserves initialized core slots and associates them with typed storage.
    WriteReservation acquire_write_reservation(size_t frame_count,
                                               bool is_shot_end)
    {
        require_initialized();
        auto reservation = core_->reserve_write(frame_count, is_shot_end);
        return WriteReservation(
            this, reservation.token_id, reservation.is_shot_start,
            reservation.is_shot_end, std::move(reservation.slots));
    }

    // Commits or cancels a reservation through the synchronized core.
    void release_write_reservation(size_t token_id,
                                   bool published)
    {
        core_->release_write_reservation(token_id, published);
    }

    // Releases a consumed window through the synchronized core.
    void release_read_window(size_t token_id)
    {
        core_->release_read_window(token_id);
    }

    std::unique_ptr<PipeCore> core_;
    std::unique_ptr<T> frame_template_;
    std::vector<T> frames_;
    PipeDescriptor descriptor_;
    std::string configuration_error_;
};
} // namespace takt
