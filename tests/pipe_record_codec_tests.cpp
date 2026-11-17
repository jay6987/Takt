// Verifies trivial recording, codec requirements, and recording cleanup errors.
#include "pipe_tests_test_support.h"

namespace
{
// Records three trivially copyable integer frames through a scoped binding.
void record_trivial_frames(const std::string& file_path)
{
    takt::Pipe<int> pipe("record-int");
    configure_pipe(pipe, 8, 0, 1, 0, 3);
    pipe.initialize();
    auto record = takt::recordreplay::make_scoped_record(pipe, file_path);
    ASSERT_TRUE(record.active());
    auto write = pipe.acquire_shot_end_write(3);
    write.value(0) = 101;
    write.value(1) = 202;
    write.value(2) = 303;
    write.publish();
}

// Confirms replay replaces seed frames with the recorded integer values.
void replay_trivial_frames(const std::string& file_path)
{
    takt::Pipe<int> pipe("replay-int");
    configure_pipe(pipe, 8, 0, 3, 0, 3);
    pipe.initialize();
    auto replay = takt::recordreplay::make_scoped_replay(pipe, file_path);
    ASSERT_TRUE(replay.active());
    {
        auto write = pipe.acquire_shot_end_write(3);
        write.value(0) = -1;
        write.value(1) = -1;
        write.value(2) = -1;
        write.publish();
    }

    auto read = pipe.acquire_read();
    EXPECT_EQ(read.value(0), 101);
    EXPECT_EQ(read.value(1), 202);
    EXPECT_EQ(read.value(2), 303);
}
} // namespace

// Verifies raw-byte recording and replay of trivially copyable frames.
TEST(PipeTests,
     ExternalRecordReplayForTrivialType)
{
    const auto file_path = (std::filesystem::current_path() /
                            "takt_record_replay_trivial_test.bin")
                               .string();

    ASSERT_NO_FATAL_FAILURE(record_trivial_frames(file_path));
    ASSERT_NO_FATAL_FAILURE(replay_trivial_frames(file_path));
    std::filesystem::remove(file_path);
}

// Verifies nontrivial payloads require explicit serialization callbacks.
TEST(PipeTests,
     ExternalRecordReplayRequiresCodecForNonTrivialType)
{
    const auto file_path = (std::filesystem::current_path() /
                            "takt_record_replay_nontrivial_test.bin")
                               .string();

    takt::Pipe<NonTrivialPayload> pipe("nontrivial");
    configure_pipe(pipe, 2, NonTrivialPayload{});
    pipe.initialize();
    EXPECT_THROW(takt::recordreplay::make_scoped_record(pipe, file_path),
                 std::runtime_error);
}

// Verifies explicit recording cleanup propagates stream errors.
TEST(PipeTests,
     ScopedRecordResetReportsStreamFailure)
{
    register_failing_record_codec();

    const auto file_path = (std::filesystem::current_path() /
                            "takt_record_replay_failed_reset.bin")
                               .string();
    takt::Pipe<FailingRecordPayload> pipe("failed-record-reset");
    configure_pipe(pipe, 2, FailingRecordPayload{});
    pipe.initialize();
    auto record = takt::recordreplay::make_scoped_record(pipe, file_path);

    {
        auto write = pipe.acquire_shot_end_write(1);
        write.value().text = "trigger stream failure";
        write.publish();
    }

    EXPECT_THROW(record.reset(), std::runtime_error);
    EXPECT_TRUE(record.active());
    EXPECT_NO_THROW(record.reset());
    EXPECT_FALSE(record.active());
    std::filesystem::remove(file_path);
}
