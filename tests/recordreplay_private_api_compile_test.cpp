#include "takt/recordreplay/recordreplay.h"

void test_registry_binding_access(takt::Pipe<int>& pipe)
{
    using Registry = takt::recordreplay::detail::RecordReplayRegistry;
    auto& registry = Registry::instance();

#if defined(TAKT_TEST_PUBLIC_API_CONTROL)
    static_cast<void>(pipe);
    static_cast<void>(registry);
#elif defined(TAKT_TEST_SET_RECORD_PRIVATE)
    registry.set_record(pipe, "record.bin");
#elif defined(TAKT_TEST_SET_REPLAY_PRIVATE)
    registry.set_replay(pipe, "replay.bin");
#elif defined(TAKT_TEST_CLEAR_RECORD_PRIVATE)
    registry.clear_record(pipe, 1);
#elif defined(TAKT_TEST_CLEAR_REPLAY_PRIVATE)
    registry.clear_replay(pipe, 1);
#else
#error Define one registry API test selector
#endif
}
