// Defines core write-reservation and read-window metadata.
#pragma once

#include <cstddef>
#include <vector>

namespace takt
{
/// Producer reservation returned by the synchronized pipe core.
struct PipeCoreWriteReservation
{
    size_t token_id = 0;
    size_t start_frame_index = 0;
    bool is_shot_start = false;
    bool is_shot_end = false;
    std::vector<size_t> slots;
};

/// Consumer window returned by the synchronized pipe core.
struct PipeCoreFrameWindow
{
    size_t token_id = 0;
    size_t start_frame_index = 0;
    bool is_shot_start = false;
    bool is_shot_end = false;
    std::vector<size_t> slots;
    size_t frame_count = 0;
    size_t overlap_frames = 0;
    size_t new_frame_count = 0;
};
} // namespace takt
