// Verifies concurrent pipe operations preserve each frame exactly once.
#include "pipe_tests_test_support.h"

namespace
{
// Starts concurrent writers with unique values and last-writer closure.
void start_integrity_writers(takt::Pipe<int>& pipe,
                             int total,
                             int writer_count,
                             std::atomic<int>& next_value,
                             std::atomic<int>& alive_writers,
                             std::vector<std::future<void>>& writers)
{
    auto* pipe_ptr = &pipe;
    auto* next_value_ptr = &next_value;
    auto* alive_writers_ptr = &alive_writers;
    for (int i = 0; i < writer_count; ++i)
    {
        writers.push_back(std::async(
            std::launch::async,
            [pipe_ptr, total, next_value_ptr, alive_writers_ptr]
            {
                while (true)
                {
                    const int value =
                        next_value_ptr->fetch_add(1, std::memory_order_relaxed);
                    if (value >= total)
                        break;
                    auto write = pipe_ptr->acquire_shot_end_write(1);
                    write.value() = value;
                    write.publish();
                }
                if (alive_writers_ptr->fetch_sub(
                        1, std::memory_order_acq_rel) == 1)
                    pipe_ptr->close();
            }));
    }
}

// Collects concurrent reads under the shared result-vector lock.
void start_integrity_readers(takt::Pipe<int>& pipe,
                             int reader_count,
                             std::vector<int>& consumed,
                             std::mutex& consumed_mutex,
                             std::vector<std::future<void>>& readers)
{
    auto* pipe_ptr = &pipe;
    auto* consumed_ptr = &consumed;
    auto* consumed_mutex_ptr = &consumed_mutex;
    for (int i = 0; i < reader_count; ++i)
    {
        readers.push_back(std::async(
            std::launch::async,
            [pipe_ptr, consumed_ptr, consumed_mutex_ptr]
            {
                while (true)
                {
                    try
                    {
                        auto read = pipe_ptr->acquire_read();
                        std::lock_guard<std::mutex> lock(*consumed_mutex_ptr);
                        consumed_ptr->push_back(read.value());
                    }
                    catch (const takt::PipeClosedAndEmptySignal&)
                    {
                        break;
                    }
                }
            }));
    }
}

// Confirms every expected frame value was delivered exactly once.
void verify_integrity_values(const std::vector<int>& consumed,
                             int total)
{
    ASSERT_EQ(consumed.size(), static_cast<size_t>(total));
    std::vector<int> counts(total, 0);
    for (int value : consumed)
    {
        ASSERT_GE(value, 0);
        ASSERT_LT(value, total);
        ++counts[static_cast<size_t>(value)];
    }
    for (int count : counts)
        EXPECT_EQ(count, 1);
}
} // namespace

// Verifies concurrent slot ownership preserves all frame values.
TEST(PipeTests,
     ConcurrentWritersReadersPreserveDataIntegrity)
{
    constexpr int kTotal = 5000;
    constexpr int kWriters = 4;
    constexpr int kReaders = 6;

    takt::Pipe<int> pipe("concurrent-integrity");
    configure_pipe(pipe, 64, 0);
    pipe.initialize();
    std::atomic<int> next_value{0};
    std::atomic<int> alive_writers{kWriters};

    std::vector<int> consumed;
    consumed.reserve(kTotal);
    std::mutex consumed_mutex;

    std::vector<std::future<void>> writers;
    writers.reserve(kWriters);
    std::vector<std::future<void>> readers;
    readers.reserve(kReaders);
    start_integrity_writers(pipe, kTotal, kWriters, next_value, alive_writers,
                            writers);
    start_integrity_readers(pipe, kReaders, consumed, consumed_mutex, readers);
    for (auto& writer : writers)
        writer.get();
    for (auto& reader : readers)
        reader.get();
    verify_integrity_values(consumed, kTotal);
}
