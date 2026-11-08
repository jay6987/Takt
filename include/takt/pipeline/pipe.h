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
    class WriteBatchToken;
    class ReadToken;

    explicit Pipe(std::string name) : PipeBase(std::move(name)) {}

    void set_producer(ProducerConfig config, T basket_template)
    {
        require_configuring();
        if (has_producer())
            fail("producer is already configured");
        auto storage = std::make_unique<T>(std::move(basket_template));
        set_producer_config(std::move(config));
        basket_template_ = std::move(storage);
    }

    const T& basket_template() const
    {
        if (!basket_template_)
            fail("basket template is only available during configuration");
        return *basket_template_;
    }

    void set_descriptor(PipeDescriptor descriptor)
    {
        require_configuring();
        descriptor_ = std::move(descriptor);
    }

    void validate_configuration() const override
    {
        validate_common();
        if (!basket_template_)
            fail("producer basket template is missing");
    }

    void initialize() override
    {
        validate_configuration();
        const auto count = capacity();
        auto core = std::make_unique<PipeCore>(
            name(), count, consumer_config().overlap_size,
            producer_config().write_size);
        std::vector<T> baskets(count, *basket_template_);
        core_ = std::move(core);
        baskets_ = std::move(baskets);
        basket_template_.reset();
        initialized_ = true;
    }

    Pipe(const Pipe&) = delete;
    Pipe& operator=(const Pipe&) = delete;

    size_t size() const noexcept
    {
        return core_ ? core_->size() : 0;
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
        auto token = acquire_write_token(producer_config().write_size, false);
        return WriteToken(std::move(token));
    }

    WriteToken acquire_shot_end_write(size_t count)
    {
        auto token = acquire_write_token(count, true);
        return WriteToken(std::move(token));
    }

    ReadToken acquire_read()
    {
        require_initialized();
        const auto& config = consumer_config();
        auto core_batch =
            core_->acquire_read_window(config.read_size, config.overlap_size);

        try
        {
            recordreplay::detail::RecordReplayRegistry::instance().replay_slots(
                *this, core_batch.slots, baskets_);

            std::vector<size_t> slots;
            slots.reserve(core_batch.read_size);
            for (size_t i = 0; i < core_batch.read_size; ++i)
            {
                slots.push_back((core_batch.start_pos + i) % capacity());
            }

            return ReadToken(this, core_batch.token_id, core_batch.is_segment_start,
                             core_batch.is_segment_end, std::move(slots),
                             core_batch.overlap_size, core_batch.read_size);
        }
        catch (...)
        {
            core_->release_read_batch(core_batch.token_id);
            throw;
        }
    }

    class WriteToken
    {
      public:
        WriteToken() = default;

        explicit WriteToken(WriteBatchToken&& batch) : batch_(std::move(batch)) {}

        WriteToken(WriteToken&& rhs) noexcept : batch_(std::move(rhs.batch_)) {}

        WriteToken& operator=(WriteToken&& rhs) noexcept
        {
            if (this == &rhs)
            {
                return *this;
            }
            batch_ = std::move(rhs.batch_);
            return *this;
        }

        WriteToken(const WriteToken&) = delete;
        WriteToken& operator=(const WriteToken&) = delete;

        ~WriteToken() = default;

        size_t size() const noexcept
        {
            return batch_.size();
        }

        T& value()
        {
            if (size() != 1)
            {
                throw std::logic_error(
                    "write token contains multiple values; use value(index)");
            }
            return batch_.value(0);
        }
        const T& value() const
        {
            if (size() != 1)
            {
                throw std::logic_error(
                    "write token contains multiple values; use value(index)");
            }
            return batch_.value(0);
        }
        T& value(size_t index)
        {
            return batch_.value(index);
        }
        const T& value(size_t index) const
        {
            return batch_.value(index);
        }

        void publish() noexcept
        {
            batch_.publish();
        }
        bool published() const noexcept
        {
            return batch_.published();
        }
        bool is_segment_start() const noexcept
        {
            return batch_.is_segment_start();
        }
        bool is_segment_end() const noexcept
        {
            return batch_.is_segment_end();
        }

      private:
        WriteBatchToken batch_;
    };

    class WriteBatchToken
    {
      public:
        WriteBatchToken() = default;

        WriteBatchToken(WriteBatchToken&& rhs) noexcept
            : owner_(rhs.owner_), token_id_(rhs.token_id_),
              is_segment_start_(rhs.is_segment_start_),
              is_segment_end_(rhs.is_segment_end_), slots_(std::move(rhs.slots_)),
              published_(rhs.published_)
        {
            rhs.owner_ = nullptr;
        }

        WriteBatchToken& operator=(WriteBatchToken&& rhs) noexcept
        {
            if (this == &rhs)
            {
                return *this;
            }
            release();
            owner_ = rhs.owner_;
            token_id_ = rhs.token_id_;
            is_segment_start_ = rhs.is_segment_start_;
            is_segment_end_ = rhs.is_segment_end_;
            slots_ = std::move(rhs.slots_);
            published_ = rhs.published_;
            rhs.owner_ = nullptr;
            return *this;
        }

        WriteBatchToken(const WriteBatchToken&) = delete;
        WriteBatchToken& operator=(const WriteBatchToken&) = delete;

        ~WriteBatchToken()
        {
            release();
        }

        size_t size() const noexcept
        {
            return slots_.size();
        }

        T& value(size_t index)
        {
            if (index >= slots_.size())
            {
                throw std::out_of_range("write batch index out of range");
            }
            return owner_->baskets_[slots_[index]];
        }

        const T& value(size_t index) const
        {
            if (index >= slots_.size())
            {
                throw std::out_of_range("write batch index out of range");
            }
            return owner_->baskets_[slots_[index]];
        }

        void publish() noexcept
        {
            published_ = true;
        }
        bool published() const noexcept
        {
            return published_;
        }
        bool is_segment_start() const noexcept
        {
            return is_segment_start_;
        }
        bool is_segment_end() const noexcept
        {
            return is_segment_end_;
        }

      private:
        friend class Pipe<T>;
        WriteBatchToken(Pipe<T>* owner, size_t token_id, bool is_segment_start,
                        bool is_segment_end, std::vector<size_t>&& slots)
            : owner_(owner), token_id_(token_id), is_segment_start_(is_segment_start),
              is_segment_end_(is_segment_end), slots_(std::move(slots))
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
                    *owner_, slots_, owner_->baskets_);
            }
            owner_->release_write_batch(token_id_, published_);
            owner_ = nullptr;
        }

        Pipe<T>* owner_ = nullptr;
        size_t token_id_ = 0;
        bool is_segment_start_ = false;
        bool is_segment_end_ = false;
        std::vector<size_t> slots_;
        bool published_ = false;
    };

    class ReadToken
    {
      public:
        ReadToken() = default;

        ReadToken(ReadToken&& rhs) noexcept
            : owner_(rhs.owner_), token_id_(rhs.token_id_),
              is_segment_start_(rhs.is_segment_start_),
              is_segment_end_(rhs.is_segment_end_), slots_(std::move(rhs.slots_)),
              overlap_size_(rhs.overlap_size_), read_size_(rhs.read_size_)
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
            is_segment_start_ = rhs.is_segment_start_;
            is_segment_end_ = rhs.is_segment_end_;
            slots_ = std::move(rhs.slots_);
            overlap_size_ = rhs.overlap_size_;
            read_size_ = rhs.read_size_;
            rhs.owner_ = nullptr;
            return *this;
        }

        ReadToken(const ReadToken&) = delete;
        ReadToken& operator=(const ReadToken&) = delete;

        ~ReadToken()
        {
            release();
        }

        size_t size() const noexcept
        {
            return read_size_;
        }
        size_t overlap_size() const noexcept
        {
            return overlap_size_;
        }
        bool is_segment_start() const noexcept
        {
            return is_segment_start_;
        }
        bool is_segment_end() const noexcept
        {
            return is_segment_end_;
        }

        T& mutable_value(size_t index)
        {
            if (index >= read_size_)
            {
                throw std::out_of_range("read token index out of range");
            }
            return owner_->baskets_[slots_[index]];
        }

        const T& value(size_t index) const
        {
            if (index >= read_size_)
            {
                throw std::out_of_range("read token index out of range");
            }
            return owner_->baskets_[slots_[index]];
        }

        T& mutable_value()
        {
            if (read_size_ != 1)
            {
                throw std::logic_error(
                    "read token contains multiple values; use mutable_value(index)");
            }
            return mutable_value(0);
        }

        const T& value() const
        {
            if (read_size_ != 1)
            {
                throw std::logic_error(
                    "read token contains multiple values; use value(index)");
            }
            return value(0);
        }

      private:
        friend class Pipe<T>;
        ReadToken(Pipe<T>* owner, size_t token_id, bool is_segment_start,
                  bool is_segment_end, std::vector<size_t>&& slots,
                  size_t overlap_size, size_t read_size)
            : owner_(owner), token_id_(token_id), is_segment_start_(is_segment_start),
              is_segment_end_(is_segment_end), slots_(std::move(slots)),
              overlap_size_(overlap_size), read_size_(read_size)
        {
        }

        void release()
        {
            if (!owner_)
            {
                return;
            }
            owner_->release_read_batch(token_id_);
            owner_ = nullptr;
        }

        Pipe<T>* owner_ = nullptr;
        size_t token_id_ = 0;
        bool is_segment_start_ = false;
        bool is_segment_end_ = false;
        std::vector<size_t> slots_;
        size_t overlap_size_ = 0;
        size_t read_size_ = 0;
    };

  private:
    WriteBatchToken acquire_write_token(size_t count, bool is_segment_end)
    {
        require_initialized();
        auto batch = core_->acquire_write_batch(count, is_segment_end);
        return WriteBatchToken(this, batch.token_id, batch.is_segment_start,
                               batch.is_segment_end, std::move(batch.slots));
    }

    void release_write_batch(size_t token_id, bool published)
    {
        core_->release_write_batch(token_id, published);
    }

    void release_read_batch(size_t token_id)
    {
        core_->release_read_batch(token_id);
    }

    std::unique_ptr<PipeCore> core_;
    std::unique_ptr<T> basket_template_;
    std::vector<T> baskets_;
    PipeDescriptor descriptor_;
    std::string configuration_error_;
};
} // namespace takt
