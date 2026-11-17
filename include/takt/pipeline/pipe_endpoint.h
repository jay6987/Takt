// Adapts typed pipes to checked type-erased subgraph boundary endpoints.
#pragma once

#include <any>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <string>
#include <typeinfo>

#include "takt/pipeline/any_pipe_endpoint.h"
#include "takt/pipeline/pipe.h"

namespace takt
{
/// Adapts a typed pipe to type-erased subgraph boundary operations.
template <typename T> class TypedPipeEndpoint final : public AnyPipeEndpoint
{
  public:
    // Owns a nonnull typed pipe used for all endpoint operations.
    explicit TypedPipeEndpoint(std::shared_ptr<Pipe<T>> pipe)
        : pipe_(std::move(pipe))
    {
        if (!pipe_)
        {
            throw std::invalid_argument("pipe endpoint cannot be null");
        }
    }

    // Copies one readable frame into a type-erased packet.
    AnyPipePacket read() override
    {
        auto token = pipe_->acquire_read();
        AnyPipePacket packet;
        packet.value = token.value();
        packet.is_shot_end = token.is_shot_end();
        return packet;
    }

    // Checks packet type before copying and publishing a writable frame.
    void write(const AnyPipePacket& packet) override
    {
        const T* typed_value = std::any_cast<T>(&(packet.value));
        if (!typed_value)
        {
            std::ostringstream oss;
            oss << "pipe endpoint type mismatch on '" << debug_name()
                << "': expected='" << typeid(T).name() << "' actual='";
            if (packet.value.has_value())
            {
                oss << packet.value.type().name();
            }
            else
            {
                oss << "<empty-any>";
            }
            oss << "'";
            throw PipeEndpointTypeMismatchError(oss.str());
        }

        auto token = packet.is_shot_end ? pipe_->acquire_shot_end_write(1)
                                        : pipe_->acquire_write();
        token.value() = *typed_value;
        token.publish();
    }

    // Forwards normal closure to the backing pipe.
    void close() override
    {
        pipe_->close();
    }

    // Forwards diagnostic closure to the backing pipe.
    void close_with_error(std::string message) override
    {
        pipe_->close_with_error(std::move(message));
    }

    // Swaps typed payload buffers between distinct compatible backing pipes.
    void transfer_to(AnyPipeEndpoint& target) override
    {
        auto* typed_target = dynamic_cast<TypedPipeEndpoint<T>*>(&target);
        if (typed_target && pipe_ == typed_target->pipe_)
        {
            throw std::invalid_argument("cannot bridge a pipe to itself");
        }
        auto read_token = pipe_->acquire_read();
        if (!typed_target)
        {
            throw PipeEndpointTypeMismatchError(
                "pipe endpoint type mismatch: " + debug_name() + " -> " +
                target.debug_name());
        }
        auto write_token = read_token.is_shot_end()
                               ? typed_target->pipe_->acquire_shot_end_write(1)
                               : typed_target->pipe_->acquire_write();
        using std::swap;
        swap(read_token.mutable_value(), write_token.value());
        write_token.publish();
    }

    // Returns the backing pipe's configured diagnostic name.
    std::string debug_name() const override
    {
        return pipe_->name();
    }

    // Returns the backing pipe for runtime lifecycle traversal.
    std::shared_ptr<PipeBase> lifecycle_pipe() const override
    {
        return pipe_;
    }

    // Accepts direct mapping only to the same typed pipe adapter.
    bool
    supports_direct_to(const AnyPipeEndpoint& target) const noexcept override
    {
        const auto* typed = dynamic_cast<const TypedPipeEndpoint<T>*>(&target);
        return typed && pipe_ == typed->pipe_;
    }

  private:
    std::shared_ptr<Pipe<T>> pipe_;
};

// Constructs a checked type-erased adapter retaining ownership of the pipe.
template <typename T>
std::shared_ptr<AnyPipeEndpoint>
make_pipe_endpoint(std::shared_ptr<Pipe<T>> pipe)
{
    return std::make_shared<TypedPipeEndpoint<T>>(std::move(pipe));
}
} // namespace takt
