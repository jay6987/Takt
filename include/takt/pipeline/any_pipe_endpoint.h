// Defines type-erased pipe endpoints and boundary transport classification.
#pragma once

#include <any>
#include <memory>
#include <stdexcept>
#include <string>

#include "takt/pipeline/channel.h"
#include "takt/pipeline/pipe_base.h"

namespace takt
{
/// Reports an incompatible payload at a type-erased pipe boundary.
struct PipeEndpointTypeMismatchError : public std::runtime_error
{
    // Preserves expected and actual type diagnostics.
    explicit PipeEndpointTypeMismatchError(const std::string& message)
        : std::runtime_error(message)
    {
    }
};

/// Type-erased frame payload and its shot boundary marker.
struct AnyPipePacket
{
    std::any value;
    bool is_shot_end = false;
};

/// Abstract frame transport and lifecycle interface for subgraph boundaries.
class AnyPipeEndpoint
{
  public:
    // Releases concrete adapters polymorphically.
    virtual ~AnyPipeEndpoint() = default;
    // Reads a frame and shot-end marker.
    virtual AnyPipePacket read() = 0;
    // Writes a frame and shot-end marker.
    virtual void write(const AnyPipePacket& packet) = 0;
    // Closes the backing pipe.
    virtual void close() = 0;
    // Closes the backing pipe with a diagnostic.
    virtual void close_with_error(std::string message) = 0;
    // Transfers a packet through the generic interface.
    virtual void transfer_to(AnyPipeEndpoint& target)
    {
        target.write(read());
    }
    // Returns the backing pipe's diagnostic name.
    virtual std::string debug_name() const = 0;
    // Exposes the backing pipe for lifecycle management.
    virtual std::shared_ptr<PipeBase> lifecycle_pipe() const = 0;
    // Reports whether direct mapping to another endpoint is safe.
    virtual bool supports_direct_to(const AnyPipeEndpoint&) const noexcept
    {
        return false;
    }
};

/// Classifies a boundary mapping, rejecting unsafe direct connections.
inline MappingTransportInfo classify_mapping(const AnyPipeEndpoint& source,
                                             const AnyPipeEndpoint& target)
{
    const auto source_pipe = source.lifecycle_pipe();
    const auto target_pipe = target.lifecycle_pipe();
    if (!source_pipe || !target_pipe)
        throw std::invalid_argument(
            "subgraph endpoint lifecycle pipe cannot be null");
    auto mode = MappingTransport::Bridge;
    if (source_pipe == target_pipe)
    {
        if (!source.supports_direct_to(target) ||
            !target.supports_direct_to(source))
            throw std::invalid_argument(
                "endpoints do not support direct mapping of pipe='" +
                source_pipe->name() + "'");
        mode = MappingTransport::Direct;
    }
    return {mode, source_pipe->name(), target_pipe->name()};
}
} // namespace takt
