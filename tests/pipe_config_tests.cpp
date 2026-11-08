#include "takt/takt.h"

#include <gtest/gtest.h>

#include <type_traits>

namespace
{
template <typename Argument>
constexpr bool is_strong_argument = std::is_constructible_v<Argument, size_t> &&
                                    !std::is_convertible_v<size_t, Argument> &&
                                    !std::is_convertible_v<Argument, size_t>;

static_assert(is_strong_argument<takt::Concurrency>);
static_assert(is_strong_argument<takt::FramesPerWrite>);
static_assert(is_strong_argument<takt::FramesPerRead>);
static_assert(is_strong_argument<takt::Overlap>);
static_assert(
    !std::is_constructible_v<takt::ProducerConfig, std::string, size_t, size_t>);
static_assert(!std::is_constructible_v<takt::ConsumerConfig, std::string, size_t,
                                       size_t, size_t>);
static_assert(!std::is_aggregate_v<takt::ProducerConfig>);
static_assert(!std::is_aggregate_v<takt::ConsumerConfig>);
static_assert(takt::concurrency(3).value == 3);
static_assert(takt::frames_per_write(5).value == 5);
static_assert(takt::frames_per_read(7).value == 7);
static_assert(takt::overlap(2).value == 2);
} // namespace

TEST(PipeConfigTests, NamedArgumentsPreserveNumericFields)
{
    takt::Pipe<int> pipe("named");
    pipe.set_producer({"source", takt::concurrency(2), takt::frames_per_write(4)}, 9);
    pipe.set_consumer(
        {"sink", takt::concurrency(3), takt::frames_per_read(5), takt::overlap(2)});

    const auto& producer = pipe.producer_config();
    EXPECT_EQ(producer.name, "source");
    EXPECT_EQ(producer.concurrency, 2U);
    EXPECT_EQ(producer.write_size, 4U);
    const auto& consumer = pipe.consumer_config();
    EXPECT_EQ(consumer.name, "sink");
    EXPECT_EQ(consumer.concurrency, 3U);
    EXPECT_EQ(consumer.read_size, 5U);
    EXPECT_EQ(consumer.overlap_size, 2U);
    EXPECT_EQ(pipe.capacity(), 11U);
}

TEST(PipeConfigTests, DefaultConstructionAndNameOnlyRetainDefaults)
{
    for (const takt::ProducerConfig config :
         {takt::ProducerConfig{}, takt::ProducerConfig{"source"}})
    {
        EXPECT_EQ(config.concurrency, 1U);
        EXPECT_EQ(config.write_size, 1U);
    }
    for (const takt::ConsumerConfig config :
         {takt::ConsumerConfig{}, takt::ConsumerConfig{"sink"}})
    {
        EXPECT_EQ(config.concurrency, 1U);
        EXPECT_EQ(config.read_size, 1U);
        EXPECT_EQ(config.overlap_size, 0U);
    }

    takt::Pipe<int> pipe("defaults");
    pipe.set_producer({"source"}, 0);
    pipe.set_consumer({"sink"});
    EXPECT_EQ(pipe.producer_config().name, "source");
    EXPECT_EQ(pipe.consumer_config().name, "sink");
    EXPECT_EQ(pipe.capacity(), 1U);
    pipe.initialize();
    {
        auto write = pipe.acquire_shot_end_write(1);
        EXPECT_EQ(write.size(), 1U);
        write.value(0) = 42;
        write.publish();
    }
    auto read = pipe.acquire_read();
    EXPECT_EQ(read.size(), 1U);
    EXPECT_EQ(read.overlap_size(), 0U);
    EXPECT_EQ(read.value(0), 42);
}

TEST(PipeConfigTests, OmittedTrailingArgumentsRetainDefaults)
{
    takt::Pipe<int> pipe("partial");
    pipe.set_producer({"source", takt::concurrency(2)}, 0);
    pipe.set_consumer({"sink", takt::concurrency(3)});
    EXPECT_EQ(pipe.producer_config().concurrency, 2U);
    EXPECT_EQ(pipe.producer_config().write_size, 1U);
    EXPECT_EQ(pipe.consumer_config().concurrency, 3U);
    EXPECT_EQ(pipe.consumer_config().read_size, 1U);
    EXPECT_EQ(pipe.consumer_config().overlap_size, 0U);
    EXPECT_EQ(pipe.capacity(), 3U);

    takt::Pipe<int> batch("partial-batch");
    batch.set_producer({"source", takt::concurrency(2), takt::frames_per_write(4)}, 0);
    batch.set_consumer({"sink", takt::concurrency(3), takt::frames_per_read(5)});
    EXPECT_EQ(batch.consumer_config().read_size, 5U);
    EXPECT_EQ(batch.consumer_config().overlap_size, 0U);
    EXPECT_EQ(batch.capacity(), 15U);
}
