// Exercises pipe tokens, bounded buffering, shot windows and record/replay.
#pragma once
#include "pipe_test_support.h"
#include "takt/takt.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <filesystem>
#include <future>
#include <mutex>
#include <stdexcept>
#include <streambuf>
#include <string>
#include <thread>
#include <type_traits>
#include <vector>

#include <gtest/gtest.h>

namespace
{
/// Represents the non trivial payload test fixture.
struct NonTrivialPayload
{
    std::string text;
};

/// Represents the failing record payload test fixture.
struct FailingRecordPayload
{
    std::string text;
};

/// Represents the failing sync stream buffer test fixture.
class FailingSyncStreamBuffer : public std::streambuf
{
  public:
    // Restarts the flush counter for an independent stream-failure scenario.
    void reset()
    {
        sync_count_ = 0;
    }

  protected:
    // Fails the second flush to exercise explicit recording cleanup errors.
    int sync() override
    {
        return ++sync_count_ == 2 ? -1 : 0;
    }

  private:
    int sync_count_ = 0;
};

FailingSyncStreamBuffer failing_sync_stream_buffer;

// Installs callbacks that inject deterministic record and replay stream errors.
inline void register_failing_record_codec()
{
    failing_sync_stream_buffer.reset();
    takt::recordreplay::PipeCodec<FailingRecordPayload> codec;
    codec.record_one = [](std::ostream& stream, const FailingRecordPayload&)
    {
        stream.rdbuf(&failing_sync_stream_buffer);
        stream.exceptions(std::ios::badbit | std::ios::failbit);
    };
    codec.replay_one = [](std::istream& stream, FailingRecordPayload&)
    {
        stream.exceptions(std::ios::badbit | std::ios::failbit);
        stream.setstate(std::ios::badbit);
    };
    takt::recordreplay::register_codec<FailingRecordPayload>(std::move(codec));
}
} // namespace
