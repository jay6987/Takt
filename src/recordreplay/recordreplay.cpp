// Implements synchronized record/replay registry operations.
#include "pipe_channel.h"

#include <exception>
#include <limits>
#include <utility>

namespace takt::recordreplay::detail
{
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
    std::type_index type,
    const void* pipe_id,
    const std::vector<size_t>& slots,
    const void* frames,
    size_t frame_size,
    const std::function<TypeErasedCodec()>& resolve_codec) noexcept
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

        const auto codec = resolve_codec();
        const auto* frame_bytes = static_cast<const char*>(frames);
        for (size_t slot : slots)
        {
            codec.record_one(*channel->record,
                             frame_bytes + (slot * frame_size));
        }
        channel->record->flush();
    }
    catch (...)
    {
        // Recording should not break production flow when triggered from token
        // cleanup.
    }
}

} // namespace takt::recordreplay::detail
