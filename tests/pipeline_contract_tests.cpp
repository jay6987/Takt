// Verifies frame copy accounting, closure draining, and token cancellation.
#include "pipeline_contract_tests_test_support.h"

// Verifies overlapping read windows reuse payload storage without copying it.
// Verifies overlapping read windows reuse payload storage without copying it.
TEST(PipelineContracts,
     OverlapWindowsReuseRingSlotsWithoutCopyingPayloads)
{
    takt::Pipe<CopyProbe> pipe("no-history");
    configure_pipe(pipe, 3, CopyProbe{}, 2, 1, 3);
    pipe.initialize();
    {
        auto write = pipe.acquire_shot_end_write(3);
        write.publish();
    }
    CopyProbe::copies = 0;

    auto first = pipe.acquire_read();
    auto second = pipe.acquire_read();
    ASSERT_EQ(second.overlap_frames(), 1U);
    EXPECT_EQ(&first.value(1), &second.value(0));
    EXPECT_EQ(CopyProbe::copies, 0);
    pipe.close();
}

// Verifies same-name builders are independent and release bound nodes.
TEST(PipelineContracts,
     SameNameBuildersAreIndependentAndReleaseBoundNodes)
{
    std::weak_ptr<takt::NodeBase> weak;
    {
        auto first = takt::PipelineBuilder::create("scoped");
        first.add_node("first");
        auto bound = std::make_shared<BlockedNode>(std::shared_future<void>{});
        weak = bound;
        first.bind_node("first", bound);
        auto second = takt::PipelineBuilder::create("scoped");
        EXPECT_TRUE(second.nodes().empty());
    }
    EXPECT_TRUE(weak.expired());
    auto rebuilt = takt::PipelineBuilder::create("scoped");
    EXPECT_TRUE(rebuilt.nodes().empty());
    EXPECT_FALSE(rebuilt.has_bound_node("first"));
}

// Verifies closure waits for outstanding writes before signaling completion.
TEST(PipelineContracts,
     ClosingWaitsForReservedWriterBeforeEndOfStream)
{
    takt::Pipe<int> pipe("pending");
    configure_pipe(pipe, 1, 0);
    pipe.initialize();
    std::optional<takt::Pipe<int>::WriteToken> write(pipe.acquire_write());
    auto reader = std::async(std::launch::async,
                             [&pipe]
                             {
                                 auto read = pipe.acquire_read();
                                 return read.value();
                             });
    EXPECT_EQ(reader.wait_for(50ms), std::future_status::timeout);
    EXPECT_THROW(pipe.close(), takt::PipeUnterminatedShotError);
    EXPECT_TRUE(pipe.closed());
    EXPECT_EQ(reader.wait_for(50ms), std::future_status::timeout);
    write->value() = 42;
    write->publish();
    write.reset();
    EXPECT_EQ(reader.get(), 42);
    EXPECT_THROW((void)pipe.acquire_read(), takt::PipeClosedAndEmptySignal);
}

// Verifies an unterminated closed shot drains without inventing a final marker.
TEST(PipelineContracts,
     ClosedUnterminatedShotDrainsPartialWindowWithoutShotEnd)
{
    takt::Pipe<int> pipe("partial-unended-tail");
    configure_pipe(pipe, 3, 0, 3, 0, 1);
    pipe.initialize();
    for (int value : {4, 5})
    {
        auto write = pipe.acquire_write();
        write.value() = value;
        write.publish();
    }

    EXPECT_THROW(pipe.close(), takt::PipeUnterminatedShotError);
    {
        auto tail = pipe.acquire_read();
        EXPECT_EQ(tail.frame_count(), 2U);
        EXPECT_FALSE(tail.is_shot_end());
        EXPECT_EQ(tail.value(0), 4);
        EXPECT_EQ(tail.value(1), 5);
    }
    EXPECT_THROW((void)pipe.acquire_read(), takt::PipeClosedAndEmptySignal);
}

// Verifies concurrent readable windows keep distinct consecutive storage.
TEST(PipelineContracts,
     MultipleReadTokensRetainTheirOwnConsecutiveWindows)
{
    takt::Pipe<int> pipe("retained-reads");
    configure_pipe(pipe, 2, 0);
    pipe.initialize();
    for (int value : {1, 2})
    {
        auto write = pipe.acquire_shot_end_write(1);
        write.value() = value;
        write.publish();
    }
    std::optional<takt::Pipe<int>::ReadToken> first(pipe.acquire_read());
    pipe.close();
    auto reader = std::async(std::launch::async,
                             [&pipe]
                             {
                                 auto second = pipe.acquire_read();
                                 return second.value();
                             });
    EXPECT_EQ(reader.wait_for(2s), std::future_status::ready);
    EXPECT_EQ(first->value(), 1);
    first.reset();
    EXPECT_EQ(reader.get(), 2);
}
