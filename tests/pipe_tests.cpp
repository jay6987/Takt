#include "takt/takt.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <filesystem>
#include <future>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <type_traits>
#include <vector>

#include <gtest/gtest.h>

namespace
{
struct NonTrivialPayload
{
    std::string text;
};

struct FailingRecordPayload
{
    std::string text;
};
} // namespace

TEST(PipeTests, SingleElementReadWrite)
{
    takt::set_debug_enabled(true);
    takt::debug("smoke test debug log");

    takt::Pipe<int> pipe("numbers", 2, 0);
    {
        auto wt = pipe.acquire_write(true);
        wt.value() = 42;
        wt.publish();
    }
    {
        auto rt = pipe.acquire_read();
        EXPECT_EQ(rt.value(), 42);
    }
    pipe.close();
}

TEST(PipeTests, BatchReadWrite)
{
    takt::Pipe<int> batch_pipe("batch", 8, 0);
    {
        auto wb = batch_pipe.acquire_write_batch(4, true);
        wb.value(0) = 10;
        wb.value(1) = 20;
        wb.value(2) = 30;
        wb.value(3) = 40;
        wb.publish();
    }
    {
        auto rb = batch_pipe.acquire_read_batch(4, 0);
        EXPECT_EQ(rb.size(), 4U);
        EXPECT_EQ(rb.value(0), 10);
        EXPECT_EQ(rb.value(3), 40);
    }
    batch_pipe.close();
}

TEST(PipeTests, MutableReadTokenSingleElementInplace)
{
    takt::Pipe<int> pipe("mutable-single", 4, 0);
    {
        auto w = pipe.acquire_write(true);
        w.value() = 11;
        w.publish();
    }

    {
        auto r = pipe.acquire_mutable_read();
        r.mutable_value() *= 3;
    }

    {
        auto w = pipe.acquire_write(true);
        w.value() = 7;
        w.publish();
    }

    {
        auto r = pipe.acquire_read();
        EXPECT_EQ(r.value(), 7);
    }
    pipe.close();
}

TEST(PipeTests, MutableReadTokenBatchInplace)
{
    takt::Pipe<int> pipe("mutable-batch", 8, 0);
    {
        auto w = pipe.acquire_write_batch(4, true);
        w.value(0) = 1;
        w.value(1) = 2;
        w.value(2) = 3;
        w.value(3) = 4;
        w.publish();
    }

    {
        auto r = pipe.acquire_mutable_read_batch(4, 0);
        for (size_t i = 0; i < r.size(); ++i)
        {
            r.mutable_value(i) += 10;
        }
    }

    {
        auto w = pipe.acquire_write_batch(2, true);
        w.value(0) = 9;
        w.value(1) = 8;
        w.publish();
    }

    {
        auto r = pipe.acquire_read_batch(2, 0);
        EXPECT_EQ(r.value(0), 9);
        EXPECT_EQ(r.value(1), 8);
    }
    pipe.close();
}

TEST(PipeTests, OverlapRead)
{
    takt::Pipe<int> batch_pipe("batch-overlap", 8, 0);
    {
        auto wb = batch_pipe.acquire_write_batch(6, true);
        for (size_t i = 0; i < 6; ++i)
        {
            wb.value(i) = static_cast<int>(i + 1); // 1..6
        }
        wb.publish();
    }
    {
        auto first = batch_pipe.acquire_read_batch(4, 0); // 1,2,3,4
        EXPECT_EQ(first.value(0), 1);
        EXPECT_EQ(first.value(3), 4);
    }
    {
        auto second = batch_pipe.acquire_read_batch(4, 2); // 3,4,5,6
        EXPECT_EQ(second.overlap_size(), 2U);
        EXPECT_EQ(second.value(0), 3);
        EXPECT_EQ(second.value(1), 4);
        EXPECT_EQ(second.value(2), 5);
        EXPECT_EQ(second.value(3), 6);
    }
    batch_pipe.close();
}

TEST(PipeTests, SegmentEndPreventsCrossSegmentRead)
{
    takt::Pipe<int> pipe("segment", 16, 0);

    {
        auto w1 = pipe.acquire_write_batch(3, true);
        w1.value(0) = 1;
        w1.value(1) = 2;
        w1.value(2) = 3;
        w1.publish();
    }
    {
        auto w2 = pipe.acquire_write_batch(4, true);
        w2.value(0) = 10;
        w2.value(1) = 11;
        w2.value(2) = 12;
        w2.value(3) = 13;
        w2.publish();
    }

    {
        auto r1 = pipe.acquire_read_batch(8, 0);
        EXPECT_TRUE(r1.is_segment_start());
        EXPECT_TRUE(r1.is_segment_end());
        EXPECT_EQ(r1.size(), 3U);
        EXPECT_EQ(r1.value(0), 1);
        EXPECT_EQ(r1.value(2), 3);
    }
    {
        auto r2 = pipe.acquire_read_batch(8, 0);
        EXPECT_TRUE(r2.is_segment_start());
        EXPECT_TRUE(r2.is_segment_end());
        EXPECT_EQ(r2.size(), 4U);
        EXPECT_EQ(r2.value(0), 10);
        EXPECT_EQ(r2.value(3), 13);
    }

    pipe.close();
}

TEST(PipeTests, SegmentFlagsOnWriteTokens)
{
    takt::Pipe<int> pipe("segment-flags", 8, 0);

    auto w1 = pipe.acquire_write(false);
    EXPECT_TRUE(w1.is_segment_start());
    EXPECT_FALSE(w1.is_segment_end());
    w1.value() = 1;
    w1.publish();

    auto w2 = pipe.acquire_write(true);
    EXPECT_FALSE(w2.is_segment_start());
    EXPECT_TRUE(w2.is_segment_end());
    w2.value() = 2;
    w2.publish();

    auto w3 = pipe.acquire_write(true);
    EXPECT_TRUE(w3.is_segment_start());
    EXPECT_TRUE(w3.is_segment_end());
    w3.value() = 3;
    w3.publish();
}

TEST(PipeTests, ExternalRecordReplayForTrivialType)
{
    const auto file_path =
        (std::filesystem::temp_directory_path() / "takt_record_replay_trivial_test.bin")
            .string();

    {
        takt::Pipe<int> record_pipe("record-int", 8, 0);
        auto record =
            takt::recordreplay::make_scoped_record(record_pipe, file_path);
        ASSERT_TRUE(record.active());

        {
            auto w = record_pipe.acquire_write_batch(3, true);
            w.value(0) = 101;
            w.value(1) = 202;
            w.value(2) = 303;
            w.publish();
        }
    }

    {
        takt::Pipe<int> replay_pipe("replay-int", 8, 0);
        auto replay =
            takt::recordreplay::make_scoped_replay(replay_pipe, file_path);
        ASSERT_TRUE(replay.active());

        {
            auto w = replay_pipe.acquire_write_batch(3, true);
            w.value(0) = -1;
            w.value(1) = -1;
            w.value(2) = -1;
            w.publish();
        }

        {
            auto r = replay_pipe.acquire_read_batch(3, 0);
            EXPECT_EQ(r.value(0), 101);
            EXPECT_EQ(r.value(1), 202);
            EXPECT_EQ(r.value(2), 303);
        }
    }

    std::filesystem::remove(file_path);
}

TEST(PipeTests, ExternalRecordReplayRequiresCodecForNonTrivialType)
{
    const auto file_path = (std::filesystem::temp_directory_path() /
                            "takt_record_replay_nontrivial_test.bin")
                               .string();

    takt::Pipe<NonTrivialPayload> pipe("nontrivial", 2, NonTrivialPayload{});
    EXPECT_THROW(takt::recordreplay::make_scoped_record(pipe, file_path),
                 std::runtime_error);
}

TEST(PipeTests, ScopedRecordResetReportsStreamFailure)
{
    takt::recordreplay::PipeCodec<FailingRecordPayload> codec;
    codec.record_one = [](std::ostream& stream, const FailingRecordPayload&)
    { stream.setstate(std::ios::badbit); };
    codec.replay_one = [](std::istream&, FailingRecordPayload&) {};
    takt::recordreplay::register_codec<FailingRecordPayload>(std::move(codec));

    const auto file_path =
        (std::filesystem::temp_directory_path() / "takt_record_replay_failed_reset.bin")
            .string();
    takt::Pipe<FailingRecordPayload> pipe("failed-record-reset", 2,
                                         FailingRecordPayload{});
    auto record = takt::recordreplay::make_scoped_record(pipe, file_path);

    {
        auto write = pipe.acquire_write(true);
        write.value().text = "trigger stream failure";
        write.publish();
    }

    EXPECT_THROW(record.reset(), std::runtime_error);
    EXPECT_TRUE(record.active());
    EXPECT_NO_THROW(record.reset());
    EXPECT_FALSE(record.active());
    std::filesystem::remove(file_path);
}

TEST(PipeTests, ScopedRecordAndReplayAreMoveOnly)
{
    static_assert(!std::is_copy_constructible_v<takt::recordreplay::ScopedRecord<int>>);
    static_assert(!std::is_copy_assignable_v<takt::recordreplay::ScopedRecord<int>>);
    static_assert(std::is_move_constructible_v<takt::recordreplay::ScopedRecord<int>>);
    static_assert(std::is_move_assignable_v<takt::recordreplay::ScopedRecord<int>>);
    static_assert(!noexcept(std::declval<takt::recordreplay::ScopedRecord<int>&>().reset()));
    static_assert(!std::is_nothrow_move_assignable_v<takt::recordreplay::ScopedRecord<int>>);
    static_assert(std::is_nothrow_destructible_v<takt::recordreplay::ScopedRecord<int>>);

    static_assert(!std::is_copy_constructible_v<takt::recordreplay::ScopedReplay<int>>);
    static_assert(!std::is_copy_assignable_v<takt::recordreplay::ScopedReplay<int>>);
    static_assert(std::is_move_constructible_v<takt::recordreplay::ScopedReplay<int>>);
    static_assert(std::is_move_assignable_v<takt::recordreplay::ScopedReplay<int>>);
    static_assert(!noexcept(std::declval<takt::recordreplay::ScopedReplay<int>&>().reset()));
    static_assert(!std::is_nothrow_move_assignable_v<takt::recordreplay::ScopedReplay<int>>);
    static_assert(std::is_nothrow_destructible_v<takt::recordreplay::ScopedReplay<int>>);
}

TEST(PipeTests, ScopedRecordReplayResetAndMoveTransferOwnership)
{
    const auto file_path =
        (std::filesystem::temp_directory_path() / "takt_record_replay_scoped_move.bin")
            .string();

    {
        takt::Pipe<int> record_pipe("record-scoped-move", 8, 0);
        auto record =
            takt::recordreplay::make_scoped_record(record_pipe, file_path);
        EXPECT_TRUE(record.active());

        auto moved_record = std::move(record);
        EXPECT_FALSE(record.active());
        EXPECT_TRUE(moved_record.active());

        {
            auto w = record_pipe.acquire_write_batch(3, true);
            w.value(0) = 7;
            w.value(1) = 8;
            w.value(2) = 9;
            w.publish();
        }

        moved_record.reset();
        EXPECT_FALSE(moved_record.active());
        moved_record.reset();
    }

    {
        takt::Pipe<int> replay_pipe("replay-scoped-move", 8, 0);
        auto replay =
            takt::recordreplay::make_scoped_replay(replay_pipe, file_path);
        EXPECT_TRUE(replay.active());

        auto moved_replay = std::move(replay);
        EXPECT_FALSE(replay.active());
        EXPECT_TRUE(moved_replay.active());

        {
            auto w = replay_pipe.acquire_write_batch(3, true);
            w.value(0) = -1;
            w.value(1) = -1;
            w.value(2) = -1;
            w.publish();
        }

        {
            auto r = replay_pipe.acquire_read_batch(3, 0);
            EXPECT_EQ(r.value(0), 7);
            EXPECT_EQ(r.value(1), 8);
            EXPECT_EQ(r.value(2), 9);
        }

        moved_replay.reset();
        EXPECT_FALSE(moved_replay.active());
        moved_replay.reset();
    }

    std::filesystem::remove(file_path);
}

TEST(PipeTests, LaterPublishedWriteDoesNotBypassEarlierUnpublishedWrite)
{
    takt::Pipe<int> pipe("ordered-visibility", 4, 0);

    {
        auto first = pipe.acquire_write(false);
        auto second = pipe.acquire_write(true);

        first.value() = 10;
        second.value() = 20;
        second.publish();
    }

    EXPECT_THROW(pipe.acquire_read(), takt::PipeClosedAndEmptySignal);
    EXPECT_TRUE(pipe.closed());
    EXPECT_TRUE(pipe.faulted());
    EXPECT_FALSE(pipe.fault_message().empty());
}

TEST(PipeTests, EarlierUnpublishedWritePreventsFutureWriteAcquisition)
{
    takt::Pipe<int> pipe("ordered-failure", 4, 0);

    {
        auto first = pipe.acquire_write(false);
        auto second = pipe.acquire_write(true);

        first.value() = 10;
        second.value() = 20;
        second.publish();
    }

    EXPECT_THROW(pipe.acquire_write(), takt::PipeFaultedError);
    EXPECT_TRUE(pipe.faulted());
    EXPECT_NE(pipe.fault_message().find("released without publish"), std::string::npos);
}

TEST(PipeTests, WriteBlocksWhenFullUntilReadReleasesSpace)
{
    using namespace std::chrono_literals;

    takt::Pipe<int> pipe("write-blocking", 2, 0);
    {
        auto w = pipe.acquire_write_batch(2, true);
        w.value(0) = 1;
        w.value(1) = 2;
        w.publish();
    }

    auto writer = std::async(std::launch::async,
                             [&pipe]
                             {
                                 auto w = pipe.acquire_write(true);
                                 w.value() = 3;
                                 w.publish();
                             });

    EXPECT_EQ(writer.wait_for(100ms), std::future_status::timeout);

    {
        auto r = pipe.acquire_read();
        EXPECT_EQ(r.value(), 1);
    }

    EXPECT_EQ(writer.wait_for(500ms), std::future_status::ready);
    pipe.close();
}

TEST(PipeTests, ReadBlocksWhenEmptyUntilWritePublishes)
{
    using namespace std::chrono_literals;

    takt::Pipe<int> pipe("read-blocking", 2, 0);

    auto reader = std::async(std::launch::async,
                             [&pipe]
                             {
                                 auto r = pipe.acquire_read();
                                 return r.value();
                             });

    EXPECT_EQ(reader.wait_for(100ms), std::future_status::timeout);

    {
        auto w = pipe.acquire_write(true);
        w.value() = 99;
        w.publish();
    }

    EXPECT_EQ(reader.wait_for(500ms), std::future_status::ready);
    EXPECT_EQ(reader.get(), 99);
    pipe.close();
}

TEST(PipeTests, CloseReadSignalIsIdempotent)
{
    takt::Pipe<int> pipe("close-idempotent", 2, 0);
    pipe.close();

    for (int i = 0; i < 3; ++i)
    {
        EXPECT_THROW(pipe.acquire_read(), takt::PipeClosedAndEmptySignal);
    }
}

TEST(PipeTests, ConcurrentWritersReadersPreserveDataIntegrity)
{
    constexpr int kTotal = 5000;
    constexpr int kWriters = 4;
    constexpr int kReaders = 6;

    takt::Pipe<int> pipe("concurrent-integrity", 64, 0);
    std::atomic<int> next_value{0};
    std::atomic<int> alive_writers{kWriters};

    std::vector<int> consumed;
    consumed.reserve(kTotal);
    std::mutex consumed_mutex;

    std::vector<std::future<void>> writers;
    writers.reserve(kWriters);
    for (int i = 0; i < kWriters; ++i)
    {
        writers.push_back(std::async(
            std::launch::async,
            [&]
            {
                while (true)
                {
                    const int v = next_value.fetch_add(1, std::memory_order_relaxed);
                    if (v >= kTotal)
                    {
                        break;
                    }
                    auto w = pipe.acquire_write(true);
                    w.value() = v;
                    w.publish();
                }

                if (alive_writers.fetch_sub(1, std::memory_order_acq_rel) == 1)
                {
                    pipe.close();
                }
            }));
    }
    std::vector<std::future<void>> readers;
    readers.reserve(kReaders);
    for (int i = 0; i < kReaders; ++i)
    {
        readers.push_back(
            std::async(std::launch::async,
                       [&]
                       {
                           while (true)
                           {
                               try
                               {
                                   auto r = pipe.acquire_read();
                                   std::lock_guard<std::mutex> lk(consumed_mutex);
                                   consumed.push_back(r.value());
                               }
                               catch (const takt::PipeClosedAndEmptySignal&)
                               {
                                   break;
                               }
                           }
                       }));
    }

    for (auto& w : writers)
    {
        w.get();
    }
    for (auto& r : readers)
    {
        r.get();
    }

    ASSERT_EQ(consumed.size(), static_cast<size_t>(kTotal));
    std::vector<int> counts(kTotal, 0);
    for (int v : consumed)
    {
        ASSERT_GE(v, 0);
        ASSERT_LT(v, kTotal);
        ++counts[static_cast<size_t>(v)];
    }
    for (int c : counts)
    {
        EXPECT_EQ(c, 1);
    }
}

TEST(PipeTests, HighConcurrencyStressNoLossNoDuplication)
{
    constexpr int kTotal = 120000;
    constexpr int kWriters = 12;
    constexpr int kReaders = 16;

    takt::Pipe<int> pipe("high-concurrency-stress", 256, 0);
    std::atomic<int> next_value{0};
    std::atomic<int> alive_writers{kWriters};

    std::vector<int> consumed;
    consumed.reserve(kTotal);
    std::mutex consumed_mutex;

    std::vector<std::future<void>> writers;
    writers.reserve(kWriters);
    for (int i = 0; i < kWriters; ++i)
    {
        writers.push_back(std::async(
            std::launch::async,
            [&]
            {
                while (true)
                {
                    const int v = next_value.fetch_add(1, std::memory_order_relaxed);
                    if (v >= kTotal)
                    {
                        break;
                    }
                    auto w = pipe.acquire_write(true);
                    w.value() = v;
                    w.publish();
                }

                if (alive_writers.fetch_sub(1, std::memory_order_acq_rel) == 1)
                {
                    pipe.close();
                }
            }));
    }

    std::vector<std::future<void>> readers;
    readers.reserve(kReaders);
    for (int i = 0; i < kReaders; ++i)
    {
        readers.push_back(
            std::async(std::launch::async,
                       [&]
                       {
                           while (true)
                           {
                               try
                               {
                                   auto r = pipe.acquire_read();
                                   std::lock_guard<std::mutex> lk(consumed_mutex);
                                   consumed.push_back(r.value());
                               }
                               catch (const takt::PipeClosedAndEmptySignal&)
                               {
                                   break;
                               }
                           }
                       }));
    }

    for (auto& writer : writers)
    {
        writer.get();
    }
    for (auto& reader : readers)
    {
        reader.get();
    }

    ASSERT_EQ(consumed.size(), static_cast<size_t>(kTotal));
    std::sort(consumed.begin(), consumed.end());
    for (int i = 0; i < kTotal; ++i)
    {
        EXPECT_EQ(consumed[static_cast<size_t>(i)], i);
    }
}
