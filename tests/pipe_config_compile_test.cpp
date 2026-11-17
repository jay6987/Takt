// Checks strongly typed pipe configuration and rejects ambiguous arguments.
#include "takt/pipeline/pipe.h"

#if defined(TAKT_TEST_CONFIG_CONTROL)
// Compiles valid shorthand and fully specified configurations.
void test_pipe_configuration(takt::Pipe<int>& pipe)
{
    using takt::concurrency;
    using takt::frames_per_read;
    using takt::frames_per_write;
    using takt::overlap_frames;

    pipe.set_producer({"source", concurrency(2), frames_per_write(4)}, 0);
    pipe.set_consumer(
        {"sink", concurrency(3), frames_per_read(5), overlap_frames(2)});
    pipe.set_producer({"source"}, 0);
    pipe.set_consumer({"sink"});
    pipe.set_producer({"source", concurrency(2)}, 0);
    pipe.set_consumer({"sink", concurrency(3)});
    pipe.set_consumer({"sink", concurrency(3), frames_per_read(5)});
    pipe.set_producer({}, 0);
    pipe.set_consumer({});
}
#elif defined(TAKT_TEST_PRODUCER_BARE)
// Rejects untyped producer configuration values.
void test_pipe_configuration(takt::Pipe<int>& pipe)
{
    pipe.set_producer({"source", 1, 1}, 0);
}
#elif defined(TAKT_TEST_CONSUMER_BARE)
// Rejects untyped consumer configuration values.
void test_pipe_configuration(takt::Pipe<int>& pipe)
{
    pipe.set_consumer({"sink", 1, 1, 0});
}
#elif defined(TAKT_TEST_PRODUCER_BARE_CONCURRENCY)
// Rejects a bare producer concurrency value.
void test_pipe_configuration(takt::Pipe<int>& pipe)
{
    using takt::frames_per_write;
    pipe.set_producer({"source", 1, frames_per_write(1)}, 0);
}
#elif defined(TAKT_TEST_PRODUCER_BARE_FRAMES)
// Rejects a bare producer frame-count value.
void test_pipe_configuration(takt::Pipe<int>& pipe)
{
    using takt::concurrency;
    pipe.set_producer({"source", concurrency(1), 1}, 0);
}
#elif defined(TAKT_TEST_CONSUMER_BARE_CONCURRENCY)
// Rejects a bare consumer concurrency value.
void test_pipe_configuration(takt::Pipe<int>& pipe)
{
    using takt::frames_per_read;
    using takt::overlap_frames;
    pipe.set_consumer({"sink", 1, frames_per_read(1), overlap_frames(0)});
}
#elif defined(TAKT_TEST_CONSUMER_BARE_FRAMES)
// Rejects a bare consumer frame-count value.
void test_pipe_configuration(takt::Pipe<int>& pipe)
{
    using takt::concurrency;
    using takt::overlap_frames;
    pipe.set_consumer({"sink", concurrency(1), 1, overlap_frames(0)});
}
#elif defined(TAKT_TEST_CONSUMER_BARE_OVERLAP)
// Rejects a bare consumer overlap value.
void test_pipe_configuration(takt::Pipe<int>& pipe)
{
    using takt::concurrency;
    using takt::frames_per_read;
    pipe.set_consumer({"sink", concurrency(1), frames_per_read(1), 0});
}
#elif defined(TAKT_TEST_PRODUCER_SWAPPED)
// Rejects producer arguments in the wrong order.
void test_pipe_configuration(takt::Pipe<int>& pipe)
{
    using takt::concurrency;
    using takt::frames_per_write;
    pipe.set_producer({"source", frames_per_write(1), concurrency(1)}, 0);
}
#elif defined(TAKT_TEST_CONSUMER_SWAPPED_CONCURRENCY)
// Rejects a consumer concurrency value in the wrong position.
void test_pipe_configuration(takt::Pipe<int>& pipe)
{
    using takt::concurrency;
    using takt::frames_per_read;
    using takt::overlap_frames;
    pipe.set_consumer(
        {"sink", frames_per_read(1), concurrency(1), overlap_frames(0)});
}
#elif defined(TAKT_TEST_CONSUMER_SWAPPED_OVERLAP)
// Rejects a consumer overlap value in the wrong position.
void test_pipe_configuration(takt::Pipe<int>& pipe)
{
    using takt::concurrency;
    using takt::frames_per_read;
    using takt::overlap_frames;
    pipe.set_consumer(
        {"sink", concurrency(1), overlap_frames(0), frames_per_read(1)});
}
#elif defined(TAKT_TEST_PRODUCER_READ_FRAMES)
// Rejects a read-frame tag in producer configuration.
void test_pipe_configuration(takt::Pipe<int>& pipe)
{
    using takt::concurrency;
    using takt::frames_per_read;
    pipe.set_producer({"source", concurrency(1), frames_per_read(1)}, 0);
}
#elif defined(TAKT_TEST_CONSUMER_WRITE_FRAMES)
// Rejects a write-frame tag in consumer configuration.
void test_pipe_configuration(takt::Pipe<int>& pipe)
{
    using takt::concurrency;
    using takt::frames_per_write;
    using takt::overlap_frames;
    pipe.set_consumer(
        {"sink", concurrency(1), frames_per_write(1), overlap_frames(0)});
}
#else
#error Define one pipe configuration test selector
#endif
