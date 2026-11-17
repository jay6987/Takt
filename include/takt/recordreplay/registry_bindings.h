// Adapts scoped typed bindings to private registry operations.
#pragma once

#include "takt/recordreplay/registry.h"

namespace takt::recordreplay::detail
{
template <typename T>
std::uint64_t RecordReplayRegistry::set_record(Pipe<T>& pipe,
                                               const std::string& file_path)
{
    static_cast<void>(resolve_codec_for_type<T>());
    return set_record_impl(std::type_index(typeid(T)), &pipe, file_path);
}

template <typename T>
std::uint64_t RecordReplayRegistry::set_replay(Pipe<T>& pipe,
                                               const std::string& file_path)
{
    static_cast<void>(resolve_codec_for_type<T>());
    return set_replay_impl(std::type_index(typeid(T)), &pipe, file_path);
}

template <typename T>
void RecordReplayRegistry::clear_record(Pipe<T>& pipe,
                                        std::uint64_t binding_id)
{
    clear_record_impl(std::type_index(typeid(T)), &pipe, binding_id);
}

template <typename T>
void RecordReplayRegistry::clear_replay(Pipe<T>& pipe,
                                        std::uint64_t binding_id)
{
    clear_replay_impl(std::type_index(typeid(T)), &pipe, binding_id);
}
} // namespace takt::recordreplay::detail
