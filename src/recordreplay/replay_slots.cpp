// Implements synchronized record/replay registry operations.
#include "pipe_channel.h"

#include <exception>
#include <limits>
#include <utility>

namespace takt::recordreplay::detail
{
void RecordReplayRegistry::replay_slots_impl(
    std::type_index type,
    const void* pipe_id,
    const std::vector<size_t>& slots,
    void* frames,
    size_t frame_size,
    const std::function<TypeErasedCodec()>& resolve_codec)
{
    std::shared_ptr<PipeChannel> channel;
    {
        std::lock_guard<std::mutex> lk(mutex_);
        auto it = pipes_.find(pipe_id);
        if (it == pipes_.end() || it->second->type != type)
        {
            return;
        }
        channel = it->second;
    }

    std::lock_guard<std::mutex> io_lk(channel->io_mutex);
    if (!channel->replay)
    {
        return;
    }

    const auto codec = resolve_codec();
    auto* frame_bytes = static_cast<char*>(frames);
    for (size_t slot : slots)
    {
        codec.replay_one(*channel->replay, frame_bytes + (slot * frame_size));
        if (channel->replay->fail())
        {
            throw std::runtime_error("replay stream read failed");
        }
    }
}

std::shared_ptr<RecordReplayRegistry::PipeChannel>
RecordReplayRegistry::get_or_create_pipe_channel_locked(const void* pipe_id,
                                                        std::type_index type)
{
    auto it = pipes_.find(pipe_id);
    if (it == pipes_.end())
    {
        auto channel = std::make_shared<PipeChannel>(type);
        pipes_.emplace(pipe_id, channel);
        return channel;
    }

    if (it->second->type != type)
    {
        throw std::logic_error("pipe id reused with different data type in "
                               "record/replay registry");
    }

    return it->second;
}
} // namespace takt::recordreplay::detail
