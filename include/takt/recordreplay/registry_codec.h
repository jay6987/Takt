// Adapts typed codecs to registry callbacks and supplies trivial codecs.
#pragma once

#include "takt/recordreplay/registry.h"
#include <istream>
#include <ostream>

namespace takt::recordreplay::detail
{
template <typename T>
RecordReplayRegistry::TypeErasedCodec
RecordReplayRegistry::make_erased_codec(PipeCodec<T> codec)
{
    if (!codec.record_one || !codec.replay_one)
        throw std::invalid_argument(
            "codec requires both record_one and replay_one");
    TypeErasedCodec erased;
    erased.record_one =
        [fn = std::move(codec.record_one)](std::ostream& os, const void* value)
    { fn(os, *static_cast<const T*>(value)); };
    erased.replay_one =
        [fn = std::move(codec.replay_one)](std::istream& is, void* value)
    { fn(is, *static_cast<T*>(value)); };
    return erased;
}

template <typename T>
RecordReplayRegistry::TypeErasedCodec
RecordReplayRegistry::resolve_codec_for_type() const
{
    const auto registered = find_codec_impl(std::type_index(typeid(T)));
    if (registered.has_value())
        return *registered;
    if constexpr (std::is_trivially_copyable_v<T>)
    {
        TypeErasedCodec codec;
        codec.record_one = [](std::ostream& os, const void* value)
        { os.write(reinterpret_cast<const char*>(value), sizeof(T)); };
        codec.replay_one = [](std::istream& is, void* value)
        { is.read(reinterpret_cast<char*>(value), sizeof(T)); };
        return codec;
    }
    else
    {
        throw std::runtime_error(
            "no codec registered for non-trivially-copyable type; call "
            "takt::recordreplay::register_codec<T>() first");
    }
}
} // namespace takt::recordreplay::detail
