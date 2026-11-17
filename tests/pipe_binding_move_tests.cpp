// Verifies replay ownership transfer and failed recording move assignment.
#include "pipe_tests_test_support.h"

#include <fstream>

namespace
{
// Confirms replay move assignment transfers the source file binding.
void verify_replay_move_assignment(const std::string& old_path,
                                   const std::string& source_path)
{
    {
        std::ofstream old_file(old_path, std::ios::binary);
        std::ofstream source_file(source_path, std::ios::binary);
        const int old_value = 1;
        const int source_value = 2718;
        old_file.write(reinterpret_cast<const char*>(&old_value),
                       sizeof(old_value));
        source_file.write(reinterpret_cast<const char*>(&source_value),
                          sizeof(source_value));
    }
    takt::Pipe<int> target_pipe("replay-move-target");
    takt::Pipe<int> source_pipe("replay-move-source");
    configure_pipe(target_pipe, 2, 0);
    configure_pipe(source_pipe, 2, 0);
    target_pipe.initialize();
    source_pipe.initialize();
    auto target = takt::recordreplay::make_scoped_replay(target_pipe, old_path);
    auto source =
        takt::recordreplay::make_scoped_replay(source_pipe, source_path);

    target = std::move(source);
    EXPECT_TRUE(target.active());
    EXPECT_FALSE(source.active());
    {
        auto seed = source_pipe.acquire_shot_end_write(1);
        seed.value() = -1;
        seed.publish();
    }
    auto read = source_pipe.acquire_read();
    EXPECT_EQ(read.value(), 2718);
    EXPECT_NO_THROW(target.reset());
}
} // namespace

// Verifies move assignment transfers exclusive replay ownership.
TEST(PipeTests,
     ScopedReplayMoveAssignmentTransfersBinding)
{
    const auto old_path =
        (std::filesystem::current_path() / "takt_replay_move_assign_old.bin")
            .string();
    const auto source_path =
        (std::filesystem::current_path() / "takt_replay_move_assign_source.bin")
            .string();
    verify_replay_move_assignment(old_path, source_path);
    std::filesystem::remove(old_path);
    std::filesystem::remove(source_path);
}

// Verifies failed destination cleanup leaves source recording ownership intact.
TEST(PipeTests,
     ScopedRecordMoveAssignmentFailurePreservesSource)
{
    register_failing_record_codec();
    const auto target_path = (std::filesystem::current_path() /
                              "takt_record_move_assign_failure.bin")
                                 .string();
    const auto source_path = (std::filesystem::current_path() /
                              "takt_record_move_assign_failure_source.bin")
                                 .string();

    {
        takt::Pipe<FailingRecordPayload> target_pipe(
            "record-move-failure-target");
        takt::Pipe<FailingRecordPayload> source_pipe(
            "record-move-failure-source");
        configure_pipe(target_pipe, 2, FailingRecordPayload{});
        configure_pipe(source_pipe, 2, FailingRecordPayload{});
        target_pipe.initialize();
        source_pipe.initialize();
        auto target =
            takt::recordreplay::make_scoped_record(target_pipe, target_path);
        auto source =
            takt::recordreplay::make_scoped_record(source_pipe, source_path);

        {
            auto write = target_pipe.acquire_shot_end_write(1);
            write.value().text = "fail target cleanup";
            write.publish();
        }

        EXPECT_THROW(target = std::move(source), std::runtime_error);
        EXPECT_TRUE(target.active());
        EXPECT_TRUE(source.active());
        EXPECT_NO_THROW(target.reset());
        EXPECT_NO_THROW(source.reset());
    }

    std::filesystem::remove(target_path);
    std::filesystem::remove(source_path);
}
