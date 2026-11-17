// Verifies failed record/replay guards cannot clear replacement bindings.
#include "pipe_tests_test_support.h"

#include <fstream>

namespace
{
// Confirms a failed recording guard cannot detach its replacement binding.
void verify_failed_record_guard(const std::string& failed_path,
                                const std::string& replacement_path,
                                const std::string& duplicate_path)
{
    register_failing_record_codec();
    takt::Pipe<FailingRecordPayload> pipe("record-stale-guard");
    configure_pipe(pipe, 2, FailingRecordPayload{});
    pipe.initialize();
    auto failed = takt::recordreplay::make_scoped_record(pipe, failed_path);
    {
        auto write = pipe.acquire_shot_end_write(1);
        write.value().text = "trigger failure";
        write.publish();
    }
    EXPECT_THROW(failed.reset(), std::runtime_error);
    auto replacement =
        takt::recordreplay::make_scoped_record(pipe, replacement_path);
    EXPECT_NO_THROW(failed.reset());
    EXPECT_THROW(takt::recordreplay::make_scoped_record(pipe, duplicate_path),
                 std::logic_error);
    EXPECT_NO_THROW(replacement.reset());
}

// Confirms a failed replay guard cannot detach its replacement binding.
void verify_failed_replay_guard(const std::string& failed_path,
                                const std::string& replacement_path,
                                const std::string& duplicate_path)
{
    register_failing_record_codec();
    for (const auto& entry :
         {std::pair{failed_path, char{1}}, std::pair{replacement_path, char{2}},
          std::pair{duplicate_path, char{3}}})
    {
        std::ofstream file(entry.first, std::ios::binary);
        file.write(&entry.second, 1);
    }
    takt::Pipe<FailingRecordPayload> pipe("replay-stale-guard");
    configure_pipe(pipe, 2, FailingRecordPayload{});
    pipe.initialize();
    auto failed = takt::recordreplay::make_scoped_replay(pipe, failed_path);
    {
        auto write = pipe.acquire_shot_end_write(1);
        write.value().text = "trigger failure";
        write.publish();
    }
    EXPECT_THROW(pipe.acquire_read(), std::runtime_error);
    EXPECT_THROW(failed.reset(), std::runtime_error);
    auto replacement =
        takt::recordreplay::make_scoped_replay(pipe, replacement_path);
    EXPECT_NO_THROW(failed.reset());
    EXPECT_THROW(takt::recordreplay::make_scoped_replay(pipe, duplicate_path),
                 std::logic_error);
    EXPECT_NO_THROW(replacement.reset());
}
} // namespace

// Verifies a stale recording scope cannot clear a newer binding.
TEST(PipeTests,
     FailedRecordGuardCannotClearReplacementBinding)
{
    const auto failed_path =
        (std::filesystem::current_path() / "takt_record_stale_guard_failed.bin")
            .string();
    const auto replacement_path = (std::filesystem::current_path() /
                                   "takt_record_stale_guard_replacement.bin")
                                      .string();
    const auto duplicate_path = (std::filesystem::current_path() /
                                 "takt_record_stale_guard_duplicate.bin")
                                    .string();

    verify_failed_record_guard(failed_path, replacement_path, duplicate_path);
    std::filesystem::remove(failed_path);
    std::filesystem::remove(replacement_path);
    std::filesystem::remove(duplicate_path);
}

// Verifies a stale replay scope cannot clear a newer binding.
TEST(PipeTests,
     FailedReplayGuardCannotClearReplacementBinding)
{
    const auto failed_path =
        (std::filesystem::current_path() / "takt_replay_stale_guard_failed.bin")
            .string();
    const auto replacement_path = (std::filesystem::current_path() /
                                   "takt_replay_stale_guard_replacement.bin")
                                      .string();
    const auto duplicate_path = (std::filesystem::current_path() /
                                 "takt_replay_stale_guard_duplicate.bin")
                                    .string();
    verify_failed_replay_guard(failed_path, replacement_path, duplicate_path);
    std::filesystem::remove(failed_path);
    std::filesystem::remove(replacement_path);
    std::filesystem::remove(duplicate_path);
}
