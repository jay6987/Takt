// Implements synchronized record/replay registry operations.
#include "pipe_channel.h"

#include <exception>
#include <limits>
#include <utility>

namespace takt::recordreplay::detail
{
namespace
{
/// Outcome retained until the owned recording binding has been cleared.
struct RecordCloseResult
{
    std::exception_ptr exception;
    bool flush_failed;
    bool close_failed;
};

// Attempts both flush and close, retaining the first cleanup exception.
RecordCloseResult close_record(std::ofstream& stream)
{
    std::exception_ptr cleanup_exception;
    try
    {
        stream.exceptions(std::ios::goodbit);
        stream.flush();
    }
    catch (...)
    {
        cleanup_exception = std::current_exception();
    }
    const bool flush_failed = stream.fail();
    try
    {
        stream.close();
    }
    catch (...)
    {
        if (!cleanup_exception)
            cleanup_exception = std::current_exception();
    }
    return {cleanup_exception, flush_failed, stream.fail()};
}
} // namespace

void RecordReplayRegistry::clear_record_impl(std::type_index type,
                                             const void* pipe_id,
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
    const auto result = close_record(*stream);
    channel->record.reset();

    if (result.exception)
    {
        std::rethrow_exception(result.exception);
    }
    if (result.flush_failed || result.close_failed)
    {
        throw std::runtime_error("failed to flush or close record stream");
    }
}

void RecordReplayRegistry::clear_replay_impl(std::type_index type,
                                             const void* pipe_id,
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
    std::exception_ptr cleanup_exception;
    try
    {
        stream->exceptions(std::ios::goodbit);
        stream->close();
    }
    catch (...)
    {
        cleanup_exception = std::current_exception();
    }
    const bool close_failed = stream->fail();
    channel->replay.reset();

    if (cleanup_exception)
    {
        std::rethrow_exception(cleanup_exception);
    }
    if (close_failed)
    {
        throw std::runtime_error("failed to close replay stream");
    }
}

} // namespace takt::recordreplay::detail
