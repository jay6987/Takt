// Defines terminal pipe signals and fault types.
#pragma once

#include <stdexcept>
#include <string>

namespace takt
{
/// Signals normal completion when a closed pipe has been drained.
struct PipeClosedAndEmptySignal : public std::runtime_error
{
    // Constructs the normal drained-pipe signal.
    PipeClosedAndEmptySignal() : std::runtime_error("pipe is closed and empty")
    {
    }
};

/// Reports closure before the current shot received an end marker.
struct PipeUnterminatedShotError : public std::runtime_error
{
    // Preserves the unterminated-shot diagnostic.
    explicit PipeUnterminatedShotError(const std::string& message)
        : std::runtime_error(message)
    {
    }
};

/// Reports a pipe's terminal failure.
struct PipeFaultedError : public std::runtime_error
{
    // Preserves the terminal pipe diagnostic.
    explicit PipeFaultedError(const std::string& message)
        : std::runtime_error(message)
    {
    }
};

/// Reports an abandoned write that prevents ordered publication.
struct PipeWriteAbandonedError : public PipeFaultedError
{
    // Preserves the abandoned-write diagnostic.
    explicit PipeWriteAbandonedError(const std::string& message)
        : PipeFaultedError(message)
    {
    }
};
} // namespace takt
