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
/// Bounded typed frame storage with move-only producer and consumer tokens.
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
    void initialize() override;

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
    void close_with_error(std::string message) override;

    // Reserves the producer's configured number of frames, blocking if full.
    WriteToken acquire_write();

    // Reserves a possibly partial batch marking the end of a shot.
    WriteToken acquire_shot_end_write(size_t frame_count);

    // Acquires a read window and replays bound frames before exposing them.
    ReadToken acquire_read();

  private:
    PipeCore* statistics_core() noexcept override { return core_.get(); }

    // Reserves initialized core slots and associates them with typed storage.
    WriteReservation acquire_write_reservation(size_t frame_count,
                                               bool is_shot_end);

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

#include "takt/pipeline/pipe_lifecycle.h"
