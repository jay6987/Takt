#include "takt/recordreplay/recordreplay.h"

#include <algorithm>
#include <limits>
#include <typeindex>
#include <utility>

namespace takt::recordreplay::detail
{
struct RecordReplayRegistry::PipeChannel
{
    explicit PipeChannel(std::type_index type_index) : type(type_index) {}

    std::type_index type;
    std::shared_ptr<std::ofstream> record;
    std::shared_ptr<std::ifstream> replay;
    std::uint64_t record_generation = 0;
    std::uint64_t replay_generation = 0;
    std::mutex io_mutex;
};

RecordReplayRegistry& RecordReplayRegistry::instance()
{
    static RecordReplayRegistry registry;
    return registry;
}

void RecordReplayRegistry::register_codec_impl(std::type_index type,
                                               TypeErasedCodec codec)
{
    std::lock_guard<std::mutex> lk(mutex_);
    codecs_[type] = std::move(codec);
}

std::uint64_t RecordReplayRegistry::set_record_impl(
    std::type_index type, const void* pipe_id, const std::string& file_path)
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

std::uint64_t RecordReplayRegistry::set_replay_impl(
    std::type_index type, const void* pipe_id, const std::string& file_path)
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

void RecordReplayRegistry::clear_record_impl(std::type_index type, const void* pipe_id,
                                            std::uint64_t binding_id)
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
    if (channel->record_generation != binding_id || !channel->record)
    {
        return;
    }

    auto stream = channel->record;
    stream->flush();
    const bool flush_failed = stream->fail();
    stream->close();
    const bool close_failed = stream->fail();
    channel->record.reset();

    if (flush_failed || close_failed)
    {
        throw std::runtime_error("failed to flush or close record stream");
    }
}

void RecordReplayRegistry::clear_replay_impl(std::type_index type, const void* pipe_id,
                                            std::uint64_t binding_id)
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
    if (channel->replay_generation != binding_id || !channel->replay)
    {
        return;
    }

    auto stream = channel->replay;
    stream->close();
    const bool close_failed = stream->fail();
    channel->replay.reset();

    if (close_failed)
    {
        throw std::runtime_error("failed to close replay stream");
    }
}

std::optional<RecordReplayRegistry::TypeErasedCodec>
RecordReplayRegistry::find_codec_impl(std::type_index type) const
{
    std::lock_guard<std::mutex> lk(mutex_);
    auto it = codecs_.find(type);
    if (it == codecs_.end())
    {
        return std::nullopt;
    }
    return it->second;
}

void RecordReplayRegistry::try_record_slots_impl(
    std::type_index type, const void* pipe_id, const std::vector<size_t>& slots,
    const void* baskets, size_t basket_size, const TypeErasedCodec& codec) noexcept
{
    try
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
        if (!channel->record)
        {
            return;
        }

        const auto* basket_bytes = static_cast<const char*>(baskets);
        for (size_t slot : slots)
        {
            codec.record_one(*channel->record, basket_bytes + (slot * basket_size));
        }
        channel->record->flush();
    }
    catch (...)
    {
        // Recording should not break production flow when triggered from token cleanup.
    }
}

void RecordReplayRegistry::replay_slots_impl(std::type_index type, const void* pipe_id,
                                             const std::vector<size_t>& slots,
                                             void* baskets, size_t basket_size,
                                             const TypeErasedCodec& codec)
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

    auto* basket_bytes = static_cast<char*>(baskets);
    for (size_t slot : slots)
    {
        codec.replay_one(*channel->replay, basket_bytes + (slot * basket_size));
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
        throw std::logic_error(
            "pipe id reused with different data type in record/replay registry");
    }

    return it->second;
}
} // namespace takt::recordreplay::detail
