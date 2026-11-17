// Adapts typed frame slots to the registry's serialized I/O operations.
#pragma once

#include "takt/recordreplay/registry.h"

namespace takt::recordreplay::detail
{
template <typename T>
void RecordReplayRegistry::try_record_slots(
    Pipe<T>& pipe,
    const std::vector<size_t>& slots,
    const std::vector<T>& frames) noexcept
{
    try
    {
        try_record_slots_impl(std::type_index(typeid(T)), &pipe, slots,
                              frames.data(), sizeof(T),
                              [this] { return resolve_codec_for_type<T>(); });
    }
    catch (...)
    {
        // Token cleanup must not propagate recording failures.
    }
}

template <typename T>
void RecordReplayRegistry::replay_slots(Pipe<T>& pipe,
                                        const std::vector<size_t>& slots,
                                        std::vector<T>& frames)
{
    replay_slots_impl(std::type_index(typeid(T)), &pipe, slots, frames.data(),
                      sizeof(T),
                      [this] { return resolve_codec_for_type<T>(); });
}
} // namespace takt::recordreplay::detail
