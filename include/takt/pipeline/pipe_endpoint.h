#pragma once

#include <any>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <string>
#include <typeinfo>

#include "takt/pipeline/channel.h"
#include "takt/pipeline/pipe.h"

namespace takt
{
struct PipeEndpointTypeMismatchError : public std::runtime_error
{
    explicit PipeEndpointTypeMismatchError(const std::string& message)
        : std::runtime_error(message)
    {
    }
};

struct AnyPipePacket
{
    std::any value;
    bool is_shot_end = false;
};

class AnyPipeEndpoint
{
  public:
    virtual ~AnyPipeEndpoint() = default;

    virtual AnyPipePacket read() = 0;
    virtual void write(const AnyPipePacket& packet) = 0;
    virtual void close() = 0;
    virtual void close_with_error(std::string message) = 0;
    virtual void transfer_to(AnyPipeEndpoint& target)
    {
        target.write(read());
    }
    virtual std::string debug_name() const = 0;
    virtual std::shared_ptr<PipeBase> lifecycle_pipe() const = 0;
    // Opt in only when sharing a pipe can bypass all endpoint operations.
    virtual bool supports_direct_to(const AnyPipeEndpoint&) const noexcept
    {
        return false;
    }
};

inline MappingTransportInfo classify_mapping(const AnyPipeEndpoint& source,
                                              const AnyPipeEndpoint& target)
{
    const auto source_pipe = source.lifecycle_pipe();
    const auto target_pipe = target.lifecycle_pipe();
    if (!source_pipe || !target_pipe)
        throw std::invalid_argument("subgraph endpoint lifecycle pipe cannot be null");
    auto mode = MappingTransport::Bridge;
    if (source_pipe == target_pipe)
    {
        if (!source.supports_direct_to(target) || !target.supports_direct_to(source))
            throw std::invalid_argument("endpoints do not support direct mapping of pipe='" +
                                        source_pipe->name() + "'");
        mode = MappingTransport::Direct;
    }
    return {mode, source_pipe->name(), target_pipe->name()};
}

template <typename T> class TypedPipeEndpoint final : public AnyPipeEndpoint
{
  public:
    explicit TypedPipeEndpoint(std::shared_ptr<Pipe<T>> pipe) : pipe_(std::move(pipe))
    {
        if (!pipe_)
        {
            throw std::invalid_argument("pipe endpoint cannot be null");
        }
    }

    AnyPipePacket read() override
    {
        auto token = pipe_->acquire_read();
        AnyPipePacket packet;
        packet.value = token.value();
        packet.is_shot_end = token.is_shot_end();
        return packet;
    }

    void write(const AnyPipePacket& packet) override
    {
        const T* typed_value = std::any_cast<T>(&(packet.value));
        if (!typed_value)
        {
            std::ostringstream oss;
            oss << "pipe endpoint type mismatch on '" << debug_name() << "': expected='"
                << typeid(T).name() << "' actual='";
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

    void close() override
    {
        pipe_->close();
    }

    void close_with_error(std::string message) override
    {
        pipe_->close_with_error(std::move(message));
    }

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

    std::string debug_name() const override
    {
        return pipe_->name();
    }

    std::shared_ptr<PipeBase> lifecycle_pipe() const override
    {
        return pipe_;
    }

    bool supports_direct_to(const AnyPipeEndpoint& target) const noexcept override
    {
        const auto* typed = dynamic_cast<const TypedPipeEndpoint<T>*>(&target);
        return typed && pipe_ == typed->pipe_;
    }

  private:
    std::shared_ptr<Pipe<T>> pipe_;
};

template <typename T>
std::shared_ptr<AnyPipeEndpoint> make_pipe_endpoint(std::shared_ptr<Pipe<T>> pipe)
{
    return std::make_shared<TypedPipeEndpoint<T>>(std::move(pipe));
}
} // namespace takt
