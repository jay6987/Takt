// Exercises the installed Takt package through public headers and linkage.
#include <takt/takt.h>

#if __has_include(<takt/pipeline/builder.h>)
#error "The installed package must not expose the removed PipeBuilder header"
#endif

// Verifies linked package behavior using only installed public interfaces.
int main()
{
    auto graph = takt::PipelineBuilder::create("installed-dot");
    graph.add_node("source");
    if (graph.to_dot().find("source") == std::string::npos)
    {
        return 1;
    }
    auto pipe = std::make_shared<takt::Pipe<int>>("installed-package");
    pipe->set_consumer({"sink", takt::concurrency(1), takt::frames_per_read(1),
                        takt::overlap_frames(0)});
    pipe->set_producer(
        {"source", takt::concurrency(1), takt::frames_per_write(1)}, 0);
    pipe->set_slot_capacity(2);
    takt::PipelineRuntime runtime({}, {pipe});
    runtime.initialize();
    runtime.start();
    {
        auto write = pipe->acquire_shot_end_write(1);
        write.value(0) = 42;
        write.publish();
    }

    auto read = pipe->acquire_read();
    runtime.join();
    return read.value(0) == 42 ? 0 : 1;
}
