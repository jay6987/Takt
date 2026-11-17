// Implements synchronized record/replay registry operations.
#include "pipe_channel.h"

#include <exception>
#include <limits>
#include <utility>

namespace takt::recordreplay::detail
{
std::uint64_t
RecordReplayRegistry::set_record_impl(std::type_index type,
                                      const void* pipe_id,
                                      const std::string& file_path)
{
    std::shared_ptr<PipeChannel> channel;
    {
        std::lock_guard<std::mutex> lk(mutex_);
        channel = get_or_create_pipe_channel_locked(pipe_id, type);
    }

    std::lock_guard<std::mutex> io_lk(channel->io_mutex);
    if (channel->record)
    {
        throw std::logic_error("record stream already active for pipe");
    }
    if (channel->record_generation == std::numeric_limits<std::uint64_t>::max())
    {
        throw std::overflow_error("record binding generation exhausted");
    }

    auto stream = std::make_shared<std::ofstream>(
        file_path, std::ios::binary | std::ios::trunc);
    if (!stream->is_open())
    {
        throw std::runtime_error("failed to open record file: " + file_path);
    }

    const auto binding_id = channel->record_generation + 1;
    channel->record = std::move(stream);
    channel->record_generation = binding_id;
    return binding_id;
}

std::uint64_t
RecordReplayRegistry::set_replay_impl(std::type_index type,
                                      const void* pipe_id,
                                      const std::string& file_path)
{
    std::shared_ptr<PipeChannel> channel;
    {
        std::lock_guard<std::mutex> lk(mutex_);
        channel = get_or_create_pipe_channel_locked(pipe_id, type);
    }

    std::lock_guard<std::mutex> io_lk(channel->io_mutex);
    if (channel->replay)
    {
        throw std::logic_error("replay stream already active for pipe");
    }
    if (channel->replay_generation == std::numeric_limits<std::uint64_t>::max())
    {
        throw std::overflow_error("replay binding generation exhausted");
    }

    auto stream = std::make_shared<std::ifstream>(file_path, std::ios::binary);
    if (!stream->is_open())
    {
        throw std::runtime_error("failed to open replay file: " + file_path);
    }

    const auto binding_id = channel->replay_generation + 1;
    channel->replay = std::move(stream);
    channel->replay_generation = binding_id;
    return binding_id;
}

} // namespace takt::recordreplay::detail
