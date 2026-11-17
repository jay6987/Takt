// Provides typed codecs and scoped recording/replay bindings for pipe frames.
#pragma once

#include <fstream>

#include "takt/recordreplay/scoped_record.h"
#include "takt/recordreplay/scoped_replay.h"

namespace takt::recordreplay
{
// Registers custom serialization for a frame type before binding file scopes.
template <typename T> void register_codec(PipeCodec<T> codec)
{
    detail::RecordReplayRegistry::instance().register_codec<T>(
        std::move(codec));
}

// Returns a move-only recording scope for the supplied pipe and file.
template <typename T>
ScopedRecord<T> make_scoped_record(Pipe<T>& pipe,
                                   std::string file_path)
{
    return ScopedRecord<T>(pipe, std::move(file_path));
}

// Returns a move-only replay scope for the supplied pipe and file.
template <typename T>
ScopedReplay<T> make_scoped_replay(Pipe<T>& pipe,
                                   std::string file_path)
{
    return ScopedReplay<T>(pipe, std::move(file_path));
}
} // namespace takt::recordreplay
