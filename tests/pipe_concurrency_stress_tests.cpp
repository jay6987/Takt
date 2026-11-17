// Stress-tests concurrent pipe writers and readers for complete frame delivery.
#include "pipe_tests_test_support.h"

namespace
{
// Starts writers that reserve unique values and close after the last writer.
void start_stress_writers(takt::Pipe<int>& pipe,
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

// Starts readers that collect values until the shared pipe drains.
void start_stress_readers(takt::Pipe<int>& pipe,
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

// Joins asynchronous participants and propagates their execution errors.
void join_tasks(std::vector<std::future<void>>& tasks)
{
    for (auto& task : tasks)
        task.get();
}

// Confirms sorting recovers exactly the expected contiguous value range.
void verify_stress_values(std::vector<int>& consumed,
                          int total)
{
    ASSERT_EQ(consumed.size(), static_cast<size_t>(total));
    std::sort(consumed.begin(), consumed.end());
    for (int i = 0; i < total; ++i)
        EXPECT_EQ(consumed[static_cast<size_t>(i)], i);
}
} // namespace

// Stress-tests concurrent buffering for lost or duplicate frames.
TEST(PipeTests,
     HighConcurrencyStressNoLossNoDuplication)
{
    constexpr int kTotal = 120000;
    constexpr int kWriters = 12;
    constexpr int kReaders = 16;

    takt::Pipe<int> pipe("high-concurrency-stress");
    configure_pipe(pipe, 256, 0);
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
    start_stress_writers(pipe, kTotal, kWriters, next_value, alive_writers,
                         writers);
    start_stress_readers(pipe, kReaders, consumed, consumed_mutex, readers);
    join_tasks(writers);
    join_tasks(readers);
    verify_stress_values(consumed, kTotal);
}
