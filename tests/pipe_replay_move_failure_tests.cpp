// Verifies replay move failures retain ownership and guards remain move-only.
#include "pipe_tests_test_support.h"

#include <fstream>

namespace
{
// Creates nonempty replay files that trigger the injected codec failure.
void create_replay_failure_files(const std::string& target_path,
                                 const std::string& source_path)
{
    std::ofstream target_file(target_path, std::ios::binary);
    std::ofstream source_file(source_path, std::ios::binary);
    const char byte = 1;
    target_file.write(&byte, 1);
    source_file.write(&byte, 1);
}

// Confirms failed target cleanup preserves both replay binding owners.
void verify_failed_replay_move_assignment(const std::string& target_path,
                                          const std::string& source_path)
{
    takt::Pipe<FailingRecordPayload> target_pipe("replay-move-failure-target");
    takt::Pipe<FailingRecordPayload> source_pipe("replay-move-failure-source");
    configure_pipe(target_pipe, 2, FailingRecordPayload{});
    configure_pipe(source_pipe, 2, FailingRecordPayload{});
    target_pipe.initialize();
    source_pipe.initialize();
    auto target =
        takt::recordreplay::make_scoped_replay(target_pipe, target_path);
    auto source =
        takt::recordreplay::make_scoped_replay(source_pipe, source_path);
    {
        auto write = target_pipe.acquire_shot_end_write(1);
        write.value().text = "trigger replay failure";
        write.publish();
    }
    EXPECT_THROW(target_pipe.acquire_read(), std::runtime_error);
    EXPECT_THROW(target = std::move(source), std::runtime_error);
    EXPECT_TRUE(target.active());
    EXPECT_TRUE(source.active());
    EXPECT_NO_THROW(target.reset());
    EXPECT_NO_THROW(source.reset());
}
} // namespace

// Verifies failed destination cleanup leaves source replay ownership intact.
TEST(PipeTests,
     ScopedReplayMoveAssignmentFailurePreservesSource)
{
    register_failing_record_codec();
    const auto target_path = (std::filesystem::current_path() /
                              "takt_replay_move_assign_failure.bin")
                                 .string();
    const auto source_path = (std::filesystem::current_path() /
                              "takt_replay_move_assign_failure_source.bin")
                                 .string();
    create_replay_failure_files(target_path, source_path);
    verify_failed_replay_move_assignment(target_path, source_path);
    std::filesystem::remove(target_path);
    std::filesystem::remove(source_path);
}

// Verifies file-binding scopes forbid copied cleanup ownership.
TEST(PipeTests,
     ScopedRecordAndReplayAreMoveOnly)
{
    static_assert(
        !std::is_copy_constructible_v<takt::recordreplay::ScopedRecord<int>>);
    static_assert(
        !std::is_copy_assignable_v<takt::recordreplay::ScopedRecord<int>>);
    static_assert(
        std::is_move_constructible_v<takt::recordreplay::ScopedRecord<int>>);
    static_assert(
        std::is_move_assignable_v<takt::recordreplay::ScopedRecord<int>>);
    static_assert(!noexcept(
        std::declval<takt::recordreplay::ScopedRecord<int>&>().reset()));
    static_assert(!std::is_nothrow_move_assignable_v<
                  takt::recordreplay::ScopedRecord<int>>);
    static_assert(
        std::is_nothrow_destructible_v<takt::recordreplay::ScopedRecord<int>>);

    static_assert(
        !std::is_copy_constructible_v<takt::recordreplay::ScopedReplay<int>>);
    static_assert(
        !std::is_copy_assignable_v<takt::recordreplay::ScopedReplay<int>>);
    static_assert(
        std::is_move_constructible_v<takt::recordreplay::ScopedReplay<int>>);
    static_assert(
        std::is_move_assignable_v<takt::recordreplay::ScopedReplay<int>>);
    static_assert(!noexcept(
        std::declval<takt::recordreplay::ScopedReplay<int>&>().reset()));
    static_assert(!std::is_nothrow_move_assignable_v<
                  takt::recordreplay::ScopedReplay<int>>);
    static_assert(
        std::is_nothrow_destructible_v<takt::recordreplay::ScopedReplay<int>>);
}
