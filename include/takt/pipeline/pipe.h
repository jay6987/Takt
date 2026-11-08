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

    explicit Pipe(std::string name) : PipeBase(std::move(name)) {}

    void set_producer(ProducerConfig config, T frame_template)
    {
        require_configuring();
        if (has_producer())
            fail("producer is already configured");
        auto storage = std::make_unique<T>(std::move(frame_template));
        set_producer_config(std::move(config));
        frame_template_ = std::move(storage);
    }

    const T& frame_template() const
    {
        if (!frame_template_)
            fail("frame template is only available during configuration");
        return *frame_template_;
    }

    void set_descriptor(PipeDescriptor descriptor)
    {
        require_configuring();
        descriptor_ = std::move(descriptor);
    }

    void validate_configuration() const override
    {
        validate_common();
        if (!frame_template_)
            fail("producer frame template is missing");
    }

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

    Pipe(const Pipe&) = delete;
    Pipe& operator=(const Pipe&) = delete;

    size_t queued_frame_count() const noexcept
    {
        return core_ ? core_->queued_frame_count() : 0;
    }
    bool closed() const noexcept override
    {
        return core_ ? core_->closed() : cancelled_;
    }
    bool faulted() const noexcept override
    {
        return core_ ? core_->faulted() : !configuration_error_.empty();
    }
    std::string fault_message() const
    {
        return core_ ? core_->fault_message() : configuration_error_;
    }
    const PipeDescriptor& descriptor() const noexcept
    {
        return descriptor_;
    }
    void close() override
    {
        if (core_)
            core_->close();
        else
            cancelled_ = true;
    }

    void close_with_error(std::string message)
    {
        if (message.empty())
            throw std::invalid_argument(context("pipe error message cannot be empty"));
        if (core_)
            core_->close_with_error(std::move(message));
        else
        {
            if (configuration_error_.empty())
                configuration_error_ = std::move(message);
            cancelled_ = true;
        }
    }

    WriteToken acquire_write()
    {
        auto token = acquire_write_reservation(producer_config().frames_per_write, false);
        return WriteToken(std::move(token));
    }

    WriteToken acquire_shot_end_write(size_t frame_count)
    {
        auto token = acquire_write_reservation(frame_count, true);
        return WriteToken(std::move(token));
    }

    ReadToken acquire_read()
    {
        require_initialized();
        const auto& config = consumer_config();
        auto window =
            core_->acquire_read_window(config.frames_per_read, config.overlap_frames);

        try
        {
            recordreplay::detail::RecordReplayRegistry::instance().replay_slots(
                *this, window.slots, frames_);

            std::vector<size_t> slots;
            slots.reserve(window.frame_count);
            for (size_t i = 0; i < window.frame_count; ++i)
            {
                slots.push_back((window.start_frame_index + i) % slot_capacity());
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
        WriteToken() = default;

        explicit WriteToken(WriteReservation&& reservation)
            : reservation_(std::move(reservation))
        {
        }

        WriteToken(WriteToken&& rhs) noexcept
            : reservation_(std::move(rhs.reservation_))
        {
        }

        WriteToken& operator=(WriteToken&& rhs) noexcept
        {
            if (this == &rhs)
            {
                return *this;
            }
            reservation_ = std::move(rhs.reservation_);
            return *this;
        }

        WriteToken(const WriteToken&) = delete;
        WriteToken& operator=(const WriteToken&) = delete;

        ~WriteToken() = default;

        size_t frame_count() const noexcept
        {
            return reservation_.frame_count();
        }

        T& value()
        {
            if (frame_count() != 1)
            {
                throw std::logic_error(
                    "write token contains multiple frames; use value(index)");
            }
            return reservation_.value(0);
        }
        const T& value() const
        {
            if (frame_count() != 1)
            {
                throw std::logic_error(
                    "write token contains multiple frames; use value(index)");
            }
            return reservation_.value(0);
        }
        T& value(size_t index)
        {
            return reservation_.value(index);
        }
        const T& value(size_t index) const
        {
            return reservation_.value(index);
        }

        void publish() noexcept
        {
            reservation_.publish();
        }
        bool published() const noexcept
        {
            return reservation_.published();
        }
        bool is_shot_start() const noexcept
        {
            return reservation_.is_shot_start();
        }
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
        WriteReservation() = default;

        WriteReservation(WriteReservation&& rhs) noexcept
            : owner_(rhs.owner_), token_id_(rhs.token_id_),
              is_shot_start_(rhs.is_shot_start_),
              is_shot_end_(rhs.is_shot_end_), slots_(std::move(rhs.slots_)),
              published_(rhs.published_)
        {
            rhs.owner_ = nullptr;
        }

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

        WriteReservation(const WriteReservation&) = delete;
        WriteReservation& operator=(const WriteReservation&) = delete;

        ~WriteReservation()
        {
            release();
        }

        size_t frame_count() const noexcept
        {
            return slots_.size();
        }

        T& value(size_t index)
        {
            if (index >= slots_.size())
            {
                throw std::out_of_range("write frame index out of range");
            }
            return owner_->frames_[slots_[index]];
        }

        const T& value(size_t index) const
        {
            if (index >= slots_.size())
            {
                throw std::out_of_range("write frame index out of range");
            }
            return owner_->frames_[slots_[index]];
        }

        void publish() noexcept
        {
            published_ = true;
        }
        bool published() const noexcept
        {
            return published_;
        }
        bool is_shot_start() const noexcept
        {
            return is_shot_start_;
        }
        bool is_shot_end() const noexcept
        {
            return is_shot_end_;
        }

      private:
        friend class Pipe<T>;
        WriteReservation(Pipe<T>* owner, size_t token_id, bool is_shot_start,
                         bool is_shot_end, std::vector<size_t>&& slots)
            : owner_(owner), token_id_(token_id), is_shot_start_(is_shot_start),
              is_shot_end_(is_shot_end), slots_(std::move(slots))
        {
        }

        void release()
        {
            if (!owner_)
            {
                return;
            }
            if (published_)
            {
                recordreplay::detail::RecordReplayRegistry::instance().try_record_slots(
                    *owner_, slots_, owner_->frames_);
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
        ReadToken() = default;

        ReadToken(ReadToken&& rhs) noexcept
            : owner_(rhs.owner_), token_id_(rhs.token_id_),
              is_shot_start_(rhs.is_shot_start_),
              is_shot_end_(rhs.is_shot_end_), slots_(std::move(rhs.slots_)),
              overlap_frames_(rhs.overlap_frames_), frame_count_(rhs.frame_count_)
        {
            rhs.owner_ = nullptr;
        }

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

        ReadToken(const ReadToken&) = delete;
        ReadToken& operator=(const ReadToken&) = delete;

        ~ReadToken()
        {
            release();
        }

        size_t frame_count() const noexcept
        {
            return frame_count_;
        }
        size_t overlap_frames() const noexcept
        {
            return overlap_frames_;
        }
        bool is_shot_start() const noexcept
        {
            return is_shot_start_;
        }
        bool is_shot_end() const noexcept
        {
            return is_shot_end_;
        }

        T& mutable_value(size_t index)
        {
            if (index >= frame_count_)
            {
                throw std::out_of_range("read frame index out of range");
            }
            return owner_->frames_[slots_[index]];
        }

        const T& value(size_t index) const
        {
            if (index >= frame_count_)
            {
                throw std::out_of_range("read frame index out of range");
            }
            return owner_->frames_[slots_[index]];
        }

        T& mutable_value()
        {
            if (frame_count_ != 1)
            {
                throw std::logic_error(
                    "read token contains multiple frames; use mutable_value(index)");
            }
            return mutable_value(0);
        }

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
        ReadToken(Pipe<T>* owner, size_t token_id, bool is_shot_start,
                  bool is_shot_end, std::vector<size_t>&& slots,
                  size_t overlap_frames, size_t frame_count)
            : owner_(owner), token_id_(token_id), is_shot_start_(is_shot_start),
              is_shot_end_(is_shot_end), slots_(std::move(slots)),
              overlap_frames_(overlap_frames), frame_count_(frame_count)
        {
        }

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
    WriteReservation acquire_write_reservation(size_t frame_count, bool is_shot_end)
    {
        require_initialized();
        auto reservation = core_->reserve_write(frame_count, is_shot_end);
        return WriteReservation(this, reservation.token_id, reservation.is_shot_start,
                                reservation.is_shot_end, std::move(reservation.slots));
    }

    void release_write_reservation(size_t token_id, bool published)
    {
        core_->release_write_reservation(token_id, published);
    }

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
