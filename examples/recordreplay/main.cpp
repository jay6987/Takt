#include <cstdio>
#include <iostream>
#include <string>

#include "takt/takt.h"

int main()
{
    // Use a local file name so the example stays self-contained across toolchains.
    const std::string file_path = "takt_recordreplay_example.bin";

    {
        // Phase 1: attach record behavior to this pipe.
        // For trivially copyable payloads like int, no custom codec is required.
        takt::Pipe<int> record_pipe("record-int", 8, 0);
        auto record =
            takt::recordreplay::make_scoped_record(record_pipe, file_path);

        // Write one batch into the pipe. Recording happens when the published write
        // token is released.
        auto w = record_pipe.acquire_write_batch(3, true);
        w.value(0) = 101;
        w.value(1) = 202;
        w.value(2) = 303;
        w.publish();
    }

    bool ok = true;
    {
        // Phase 2: attach replay behavior to a new pipe reading from the same file.
        takt::Pipe<int> replay_pipe("replay-int", 8, 0);
        auto replay =
            takt::recordreplay::make_scoped_replay(replay_pipe, file_path);

        // Reserve target slots by writing placeholders.
        // The subsequent read path replays persisted data into these slots before
        // exposing values.
        {
            auto w = replay_pipe.acquire_write_batch(3, true);
            w.value(0) = -1;
            w.value(1) = -1;
            w.value(2) = -1;
            w.publish();
        }

        // Expect replayed values to replace placeholders after the write token is
        // released.
        auto r = replay_pipe.acquire_read_batch(3, 0);
        ok = (r.value(0) == 101) && (r.value(1) == 202) && (r.value(2) == 303);
    }

    // Best-effort cleanup for the generated record file.
    std::remove(file_path.c_str());
    std::cout << "record/replay " << (ok ? "succeeded" : "failed") << std::endl;
    return ok ? 0 : 1;
}
