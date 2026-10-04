#include "takt/takt.h"

#include <string>
#include <vector>

#include <gtest/gtest.h>

TEST(PipeBuilderTests, SeparatedProducerAndConsumerCalls)
{
    auto producer_builder = takt::PipeBuilder::declare("feature-builder");
    producer_builder.producer("encoder", 4, 10, 7);
    producer_builder.descriptor_value("element_shape", std::vector<int64_t>{1024, 16});
    producer_builder.descriptor_value("dtype", std::string("int32"));

    auto consumer_builder = takt::PipeBuilder::declare("feature-builder");
    consumer_builder.consumer("trainer", 3, 5, 1);

    EXPECT_EQ(consumer_builder.capacity(), 40U);

    const auto builder_descriptor = consumer_builder.descriptor();
    const auto* shape = takt::find_descriptor_value<std::vector<int64_t>>(
        builder_descriptor, "element_shape");
    ASSERT_NE(shape, nullptr);
    EXPECT_EQ((*shape)[0], 1024);
    EXPECT_EQ((*shape)[1], 16);

    auto pipe = producer_builder.build<int>();
    EXPECT_EQ(pipe.capacity(), 40U);

    const auto* dtype =
        takt::find_descriptor_value<std::string>(pipe.descriptor(), "dtype");
    ASSERT_NE(dtype, nullptr);
    EXPECT_EQ(*dtype, "int32");
}
