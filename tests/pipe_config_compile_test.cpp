#include "takt/pipeline/pipe.h"

void test_pipe_configuration(takt::Pipe<int>& pipe)
{
    using takt::concurrency;
    using takt::frames_per_read;
    using takt::frames_per_write;
    using takt::overlap_frames;

#if defined(TAKT_TEST_CONFIG_CONTROL)
    pipe.set_producer({"source", concurrency(2), frames_per_write(4)}, 0);
    pipe.set_consumer({"sink", concurrency(3), frames_per_read(5), overlap_frames(2)});
    pipe.set_producer({"source"}, 0);
    pipe.set_consumer({"sink"});
    pipe.set_producer({"source", concurrency(2)}, 0);
    pipe.set_consumer({"sink", concurrency(3)});
    pipe.set_consumer({"sink", concurrency(3), frames_per_read(5)});
    pipe.set_producer({}, 0);
    pipe.set_consumer({});
#elif defined(TAKT_TEST_PRODUCER_BARE)
    pipe.set_producer({"source", 1, 1}, 0);
#elif defined(TAKT_TEST_CONSUMER_BARE)
    pipe.set_consumer({"sink", 1, 1, 0});
#elif defined(TAKT_TEST_PRODUCER_BARE_CONCURRENCY)
    pipe.set_producer({"source", 1, frames_per_write(1)}, 0);
#elif defined(TAKT_TEST_PRODUCER_BARE_FRAMES)
    pipe.set_producer({"source", concurrency(1), 1}, 0);
#elif defined(TAKT_TEST_CONSUMER_BARE_CONCURRENCY)
    pipe.set_consumer({"sink", 1, frames_per_read(1), overlap_frames(0)});
#elif defined(TAKT_TEST_CONSUMER_BARE_FRAMES)
    pipe.set_consumer({"sink", concurrency(1), 1, overlap_frames(0)});
#elif defined(TAKT_TEST_CONSUMER_BARE_OVERLAP)
    pipe.set_consumer({"sink", concurrency(1), frames_per_read(1), 0});
#elif defined(TAKT_TEST_PRODUCER_SWAPPED)
    pipe.set_producer({"source", frames_per_write(1), concurrency(1)}, 0);
#elif defined(TAKT_TEST_CONSUMER_SWAPPED_CONCURRENCY)
    pipe.set_consumer({"sink", frames_per_read(1), concurrency(1), overlap_frames(0)});
#elif defined(TAKT_TEST_CONSUMER_SWAPPED_OVERLAP)
    pipe.set_consumer({"sink", concurrency(1), overlap_frames(0), frames_per_read(1)});
#elif defined(TAKT_TEST_PRODUCER_READ_FRAMES)
    pipe.set_producer({"source", concurrency(1), frames_per_read(1)}, 0);
#elif defined(TAKT_TEST_CONSUMER_WRITE_FRAMES)
    pipe.set_consumer({"sink", concurrency(1), frames_per_write(1), overlap_frames(0)});
#else
#error Define one pipe configuration test selector
#endif
}
