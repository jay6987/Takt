// Holds synchronized record/replay streams and binding generations.
#pragma once

#include "takt/recordreplay/registry.h"
#include <fstream>

namespace takt::recordreplay::detail
{
/// Synchronized streams and binding generations for one frame type.
struct RecordReplayRegistry::PipeChannel
{
    // Associates synchronized recording/replay streams with one payload type.
    explicit PipeChannel(std::type_index type_index) : type(type_index) {}

    std::type_index type;
    std::shared_ptr<std::ofstream> record;
    std::shared_ptr<std::ifstream> replay;
    std::uint64_t record_generation = 0;
    std::uint64_t replay_generation = 0;
    std::mutex io_mutex;
};

} // namespace takt::recordreplay::detail
