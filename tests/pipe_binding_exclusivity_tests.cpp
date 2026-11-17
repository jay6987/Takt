// Verifies exclusive record/replay guards and recording ownership transfer.
#include "pipe_tests_test_support.h"

#include <fstream>

namespace
{
// Confirms moving a record binding records through the source pipe only.
void verify_record_move_assignment(const std::string& old_path,
                                   const std::string& source_path)
{
    takt::Pipe<int> target_pipe("record-move-target");
    takt::Pipe<int> source_pipe("record-move-source");
    configure_pipe(target_pipe, 2, 0);
    configure_pipe(source_pipe, 2, 0);
    target_pipe.initialize();
    source_pipe.initialize();
    auto target = takt::recordreplay::make_scoped_record(target_pipe, old_path);
    auto source =
        takt::recordreplay::make_scoped_record(source_pipe, source_path);

    target = std::move(source);
    EXPECT_TRUE(target.active());
    EXPECT_FALSE(source.active());

    {
        auto write = source_pipe.acquire_shot_end_write(1);
        write.value() = 314;
        write.publish();
    }
    EXPECT_NO_THROW(target.reset());

    takt::Pipe<int> replay_pipe("record-move-verify");
    configure_pipe(replay_pipe, 2, 0);
    replay_pipe.initialize();
    auto replay =
        takt::recordreplay::make_scoped_replay(replay_pipe, source_path);
    {
        {
            auto seed = replay_pipe.acquire_shot_end_write(1);
            seed.value() = -1;
            seed.publish();
        }
    }
    auto read = replay_pipe.acquire_read();
    EXPECT_EQ(read.value(), 314);
}
} // namespace

// Verifies recording scopes cannot simultaneously own the same pipe.
TEST(PipeTests,
     ScopedRecordRejectsOverlappingGuards)
{
    const auto first_path =
        (std::filesystem::current_path() / "takt_record_overlap_first.bin")
            .string();
    const auto second_path =
        (std::filesystem::current_path() / "takt_record_overlap_second.bin")
            .string();
    takt::Pipe<int> pipe("record-overlap");
    configure_pipe(pipe, 2, 0);
    pipe.initialize();
    auto record = takt::recordreplay::make_scoped_record(pipe, first_path);

    EXPECT_THROW(takt::recordreplay::make_scoped_record(pipe, second_path),
                 std::logic_error);
    EXPECT_TRUE(record.active());
    EXPECT_NO_THROW(record.reset());
    std::filesystem::remove(first_path);
    std::filesystem::remove(second_path);
}

// Verifies replay scopes cannot simultaneously own the same pipe.
TEST(PipeTests,
     ScopedReplayRejectsOverlappingGuards)
{
    const auto first_path =
        (std::filesystem::current_path() / "takt_replay_overlap_first.bin")
            .string();
    const auto second_path =
        (std::filesystem::current_path() / "takt_replay_overlap_second.bin")
            .string();
    {
        std::ofstream first_file(first_path, std::ios::binary);
        std::ofstream second_file(second_path, std::ios::binary);
    }

    takt::Pipe<int> pipe("replay-overlap");
    configure_pipe(pipe, 2, 0);
    pipe.initialize();
    auto replay = takt::recordreplay::make_scoped_replay(pipe, first_path);

    EXPECT_THROW(takt::recordreplay::make_scoped_replay(pipe, second_path),
                 std::logic_error);
    EXPECT_TRUE(replay.active());
    EXPECT_NO_THROW(replay.reset());
    std::filesystem::remove(first_path);
    std::filesystem::remove(second_path);
}

// Verifies move assignment transfers exclusive recording ownership.
TEST(PipeTests,
     ScopedRecordMoveAssignmentTransfersBinding)
{
    const auto old_path =
        (std::filesystem::current_path() / "takt_record_move_assign_old.bin")
            .string();
    const auto source_path =
        (std::filesystem::current_path() / "takt_record_move_assign_source.bin")
            .string();

    verify_record_move_assignment(old_path, source_path);

    std::filesystem::remove(old_path);
    std::filesystem::remove(source_path);
}
