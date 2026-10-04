#pragma once

#include <any>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <string>
#include <typeinfo>

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
    bool is_segment_end = false;
};

class AnyPipeEndpoint
{
  public:
    virtual ~AnyPipeEndpoint() = default;

    virtual AnyPipePacket read() = 0;
    virtual void write(const AnyPipePacket& packet) = 0;
    virtual void close() = 0;
    virtual std::string debug_name() const = 0;
};

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
        packet.is_segment_end = token.is_segment_end();
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

        auto token = pipe_->acquire_write(packet.is_segment_end);
        token.value() = *typed_value;
        token.publish();
    }

    void close() override
    {
        pipe_->close();
    }

    std::string debug_name() const override
    {
        return pipe_->name();
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
