// Declares typed frame serialization callbacks.
#pragma once

#include <functional>
#include <iosfwd>

namespace takt::recordreplay
{
/// Pair of callbacks for serializing and restoring a typed pipe frame.
template <typename T> struct PipeCodec
{
    std::function<void(std::ostream&, const T&)> record_one;
    std::function<void(std::istream&, T&)> replay_one;
};
} // namespace takt::recordreplay
