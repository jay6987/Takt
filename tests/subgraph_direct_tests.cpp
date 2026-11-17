// Verifies direct boundary selection and typed endpoint compatibility.
#include "subgraph_direct_tests_test_support.h"

// Verifies direct transport requires shared identity and mutual capability.
// Verifies direct transport requires shared identity and mutual capability.
TEST(SubgraphDirect,
     IdentityAndCustomEndpointCapability)
{
    auto first = pipe("same");
    auto second = pipe("same");
    const auto source = takt::make_pipe_endpoint(first);
    const auto alias = takt::make_pipe_endpoint(first);
    const auto other = takt::make_pipe_endpoint(second);
    EXPECT_NE(source, alias);
    EXPECT_EQ(takt::classify_mapping(*source, *alias).mode,
              takt::MappingTransport::Direct);
    EXPECT_EQ(takt::classify_mapping(*source, *other).mode,
              takt::MappingTransport::Bridge);
    CustomEndpoint custom(first);
    CustomEndpoint custom_other(second);
    CustomEndpoint null(nullptr);
    EXPECT_THROW(takt::classify_mapping(*source, custom),
                 std::invalid_argument);
    EXPECT_THROW(takt::classify_mapping(custom, custom), std::invalid_argument);
    EXPECT_THROW(takt::classify_mapping(*source, null), std::invalid_argument);
    EXPECT_EQ(takt::classify_mapping(custom, custom_other).mode,
              takt::MappingTransport::Bridge);
    CustomEndpoint custom_alias(first);
    custom.direct_supported = true;
    EXPECT_THROW(takt::classify_mapping(custom, custom_alias),
                 std::invalid_argument);
    custom_alias.direct_supported = true;
    EXPECT_EQ(takt::classify_mapping(custom, custom_alias).mode,
              takt::MappingTransport::Direct);
    first->initialize();
    second->initialize();
    {
        auto write = first->acquire_write();
        write.value() = 7;
        write.publish();
    }
    custom.transfer_to(custom_other);
    EXPECT_EQ(custom.reads, 1);
    EXPECT_EQ(custom_other.writes, 1);
    EXPECT_EQ(second->acquire_read().value(), 7);
    EXPECT_THROW(source->transfer_to(*alias), std::invalid_argument);
}
