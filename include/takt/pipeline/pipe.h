#pragma once

#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "takt/pipeline/descriptor.h"
#include "takt/pipeline/pipe_base.h"
#include "takt/pipeline/pipe_core.h"
#include "takt/recordreplay/recordreplay.h"

namespace takt
{
enum class ReadHistoryPolicy
{
    Snapshot,
    Disabled,
};

template <typename T> class Pipe : public PipeBase
{
  public:
    class WriteToken;
    class WriteBatchToken;
    class ReadToken;
    class ReadBatchToken;

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

    void set_read_history_policy(ReadHistoryPolicy policy)
    {
        require_configuring();
        history_policy_ = policy;
    }

    void validate_configuration() const override
    {
        validate_common();
        if (!basket_template_)
            fail("producer basket template is missing");
        if (history_policy_ == ReadHistoryPolicy::Disabled &&
            consumer_config().overlap_size != 0)
            throw std::invalid_argument(
                context("overlap requires Snapshot read history"));
    }

    void initialize() override
    {
        validate_configuration();
        const auto count = capacity();
        auto core = std::make_unique<PipeCore>(name(), count);
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
        auto batch = acquire_write_batch(1, true);
        return WriteToken(std::move(batch));
    }

    WriteToken acquire_write(bool is_segment_end)
    {
        auto batch = acquire_write_batch(1, is_segment_end);
        return WriteToken(std::move(batch));
    }

    WriteBatchToken acquire_write_batch(size_t count, bool is_segment_end)
    {
        require_initialized();
        auto batch = core_->acquire_write_batch(count, is_segment_end);
        return WriteBatchToken(this, batch.token_id, batch.is_segment_start,
                               batch.is_segment_end, std::move(batch.slots));
    }

    WriteBatchToken acquire_configured_write_batch(bool is_segment_end)
    {
        return acquire_write_batch(producer_config().write_size, is_segment_end);
    }

    ReadBatchToken acquire_configured_read_batch()
    {
        const auto& config = consumer_config();
        return acquire_read_batch(config.read_size, config.overlap_size);
    }

    ReadToken acquire_read()
    {
        auto batch = acquire_read_batch(1, 0);
        return ReadToken(std::move(batch));
    }

    ReadToken acquire_mutable_read()
    {
        return acquire_read();
    }

    ReadBatchToken acquire_read_batch(size_t read_size, size_t overlap_size = 0)
    {
        require_initialized();
        if (overlap_size > 0 && history_policy_ == ReadHistoryPolicy::Disabled)
        {
            throw std::invalid_argument("overlap requires Snapshot read history");
        }
        std::vector<T> last_read_snapshot;
        bool has_previous_read = false;
        size_t previous_read_size = 0;
        {
            std::lock_guard<std::mutex> lk(read_history_mutex_);
            has_previous_read = last_read_initialized_;
            previous_read_size = last_read_values_.size();
            if (overlap_size > 0 && has_previous_read)
            {
                last_read_snapshot = last_read_values_;
            }
        }

        auto core_batch = core_->acquire_read_batch(
            read_size, overlap_size, has_previous_read, previous_read_size);

        try
        {
            recordreplay::detail::RecordReplayRegistry::instance().replay_slots(
                *this, core_batch.slots, baskets_);
        }
        catch (...)
        {
            core_->release_read_batch(core_batch.token_id);
            throw;
        }

        std::vector<T> overlap_values;
        overlap_values.reserve(core_batch.overlap_size);
        for (size_t i = last_read_snapshot.size() - core_batch.overlap_size;
             i < last_read_snapshot.size(); ++i)
        {
            overlap_values.push_back(last_read_snapshot[i]);
        }

        return ReadBatchToken(this, core_batch.token_id, core_batch.is_segment_start,
                              core_batch.is_segment_end, std::move(core_batch.slots),
                              std::move(overlap_values), core_batch.overlap_size,
                              core_batch.read_size);
    }

    ReadBatchToken acquire_mutable_read_batch(size_t read_size, size_t overlap_size = 0)
    {
        return acquire_read_batch(read_size, overlap_size);
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

        T& value()
        {
            return batch_.value(0);
        }
        const T& value() const
        {
            return batch_.value(0);
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

        explicit ReadToken(ReadBatchToken&& batch) : batch_(std::move(batch)) {}

        ReadToken(ReadToken&& rhs) noexcept : batch_(std::move(rhs.batch_)) {}

        ReadToken& operator=(ReadToken&& rhs) noexcept
        {
            if (this == &rhs)
            {
                return *this;
            }
            batch_ = std::move(rhs.batch_);
            return *this;
        }

        ReadToken(const ReadToken&) = delete;
        ReadToken& operator=(const ReadToken&) = delete;

        ~ReadToken() = default;

        const T& value() const
        {
            return batch_.value(0);
        }
        T& mutable_value()
        {
            return batch_.mutable_value(0);
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
        ReadBatchToken batch_;
    };

    class ReadBatchToken
    {
      public:
        ReadBatchToken() = default;

        ReadBatchToken(ReadBatchToken&& rhs) noexcept
            : owner_(rhs.owner_), token_id_(rhs.token_id_),
              is_segment_start_(rhs.is_segment_start_),
              is_segment_end_(rhs.is_segment_end_), slots_(std::move(rhs.slots_)),
              overlap_values_(std::move(rhs.overlap_values_)),
              overlap_size_(rhs.overlap_size_), read_size_(rhs.read_size_)
        {
            rhs.owner_ = nullptr;
        }

        ReadBatchToken& operator=(ReadBatchToken&& rhs) noexcept
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
            overlap_values_ = std::move(rhs.overlap_values_);
            overlap_size_ = rhs.overlap_size_;
            read_size_ = rhs.read_size_;
            rhs.owner_ = nullptr;
            return *this;
        }

        ReadBatchToken(const ReadBatchToken&) = delete;
        ReadBatchToken& operator=(const ReadBatchToken&) = delete;

        ~ReadBatchToken()
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
                throw std::out_of_range("read batch index out of range");
            }
            if (index < overlap_size_)
            {
                return overlap_values_[index];
            }
            return owner_->baskets_[slots_[index - overlap_size_]];
        }

        const T& value(size_t index) const
        {
            if (index >= read_size_)
            {
                throw std::out_of_range("read batch index out of range");
            }
            if (index < overlap_size_)
            {
                return overlap_values_[index];
            }
            return owner_->baskets_[slots_[index - overlap_size_]];
        }

      private:
        friend class Pipe<T>;
        ReadBatchToken(Pipe<T>* owner, size_t token_id, bool is_segment_start,
                       bool is_segment_end, std::vector<size_t>&& slots,
                       std::vector<T>&& overlap_values, size_t overlap_size,
                       size_t read_size)
            : owner_(owner), token_id_(token_id), is_segment_start_(is_segment_start),
              is_segment_end_(is_segment_end), slots_(std::move(slots)),
              overlap_values_(std::move(overlap_values)), overlap_size_(overlap_size),
              read_size_(read_size)
        {
        }

        void release()
        {
            if (!owner_)
            {
                return;
            }
            owner_->release_read_batch(token_id_, slots_, overlap_values_, read_size_,
                                       overlap_size_);
            owner_ = nullptr;
        }

        Pipe<T>* owner_ = nullptr;
        size_t token_id_ = 0;
        bool is_segment_start_ = false;
        bool is_segment_end_ = false;
        std::vector<size_t> slots_;
        std::vector<T> overlap_values_;
        size_t overlap_size_ = 0;
        size_t read_size_ = 0;
    };

  private:
    void release_write_batch(size_t token_id, bool published)
    {
        core_->release_write_batch(token_id, published);
    }

    void release_read_batch(size_t token_id, const std::vector<size_t>& slots,
                            const std::vector<T>& overlap_values, size_t read_size,
                            size_t overlap_size)
    {
        if (history_policy_ == ReadHistoryPolicy::Disabled)
        {
            core_->release_read_batch(token_id);
            return;
        }
        std::vector<T> current_read_values;
        current_read_values.reserve(read_size);
        for (size_t i = 0; i < read_size; ++i)
        {
            if (i < overlap_size)
            {
                current_read_values.push_back(overlap_values[i]);
            }
            else
            {
                current_read_values.push_back(baskets_[slots[i - overlap_size]]);
            }
        }
        {
            std::lock_guard<std::mutex> lk(read_history_mutex_);
            last_read_values_ = std::move(current_read_values);
            last_read_initialized_ = true;
        }

        core_->release_read_batch(token_id);
    }

    std::unique_ptr<PipeCore> core_;
    std::unique_ptr<T> basket_template_;
    std::vector<T> baskets_;
    PipeDescriptor descriptor_;
    ReadHistoryPolicy history_policy_ = ReadHistoryPolicy::Snapshot;
    std::string configuration_error_;
    mutable std::mutex read_history_mutex_;
    std::vector<T> last_read_values_;
    bool last_read_initialized_ = false;
};
} // namespace takt
