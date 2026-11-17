// Verifies scoped binding lifetimes and ordered write-publication failures.
#include "pipe_tests_test_support.h"

namespace
{
// Checks recording move construction and repeated explicit reset.
void record_scoped_frames(const std::string& file_path)
{
    takt::Pipe<int> pipe("record-scoped-move");
    configure_pipe(pipe, 8, 0, 1, 0, 3);
    pipe.initialize();
    auto record = takt::recordreplay::make_scoped_record(pipe, file_path);
    EXPECT_TRUE(record.active());
    auto moved_record = std::move(record);
    EXPECT_FALSE(record.active());
    EXPECT_TRUE(moved_record.active());
    {
        auto write = pipe.acquire_shot_end_write(3);
        write.value(0) = 7;
        write.value(1) = 8;
        write.value(2) = 9;
        write.publish();
    }
    moved_record.reset();
    EXPECT_FALSE(moved_record.active());
    moved_record.reset();
}

// Checks replay move construction, restored values, and repeated reset.
void replay_scoped_frames(const std::string& file_path)
{
    takt::Pipe<int> pipe("replay-scoped-move");
    configure_pipe(pipe, 8, 0, 3, 0, 3);
    pipe.initialize();
    auto replay = takt::recordreplay::make_scoped_replay(pipe, file_path);
    EXPECT_TRUE(replay.active());
    auto moved_replay = std::move(replay);
    EXPECT_FALSE(replay.active());
    EXPECT_TRUE(moved_replay.active());
    {
        auto write = pipe.acquire_shot_end_write(3);
        write.value(0) = -1;
        write.value(1) = -1;
        write.value(2) = -1;
        write.publish();
    }
    {
        auto read = pipe.acquire_read();
        EXPECT_EQ(read.value(0), 7);
        EXPECT_EQ(read.value(1), 8);
        EXPECT_EQ(read.value(2), 9);
    }
    moved_replay.reset();
    EXPECT_FALSE(moved_replay.active());
    moved_replay.reset();
}
} // namespace

// Verifies explicit reset and move construction preserve binding lifetimes.
TEST(PipeTests,
     ScopedRecordReplayResetAndMoveTransferOwnership)
{
    const auto file_path =
        (std::filesystem::current_path() / "takt_record_replay_scoped_move.bin")
            .string();

    record_scoped_frames(file_path);
    replay_scoped_frames(file_path);
    std::filesystem::remove(file_path);
}

// Verifies ordered publication stops at an abandoned earlier reservation.
TEST(PipeTests,
     LaterPublishedWriteDoesNotBypassEarlierUnpublishedWrite)
{
    takt::Pipe<int> pipe("ordered-visibility");
    configure_pipe(pipe, 4, 0);
    pipe.initialize();

    {
        auto first = pipe.acquire_write();
        auto second = pipe.acquire_shot_end_write(1);

        first.value() = 10;
        second.value() = 20;
        second.publish();
    }

    EXPECT_THROW(pipe.acquire_read(), takt::PipeFaultedError);
    EXPECT_TRUE(pipe.closed());
    EXPECT_TRUE(pipe.faulted());
    EXPECT_FALSE(pipe.fault_message().empty());
}

// Verifies abandoned publication rejects later write reservations.
TEST(PipeTests,
     EarlierUnpublishedWritePreventsFutureWriteAcquisition)
{
    takt::Pipe<int> pipe("ordered-failure");
    configure_pipe(pipe, 4, 0);
    pipe.initialize();

    {
        auto first = pipe.acquire_write();
        auto second = pipe.acquire_shot_end_write(1);

        first.value() = 10;
        second.value() = 20;
        second.publish();
    }

    EXPECT_THROW(pipe.acquire_write(), takt::PipeFaultedError);
    EXPECT_TRUE(pipe.faulted());
    EXPECT_NE(pipe.fault_message().find("released without publish"),
              std::string::npos);
}
