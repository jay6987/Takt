#include <takt/takt.h>

#if __has_include(<takt/pipeline/builder.h>)
#error "The installed package must not expose the removed PipeBuilder header"
#endif

int main()
{
    auto graph = takt::PipelineBuilder::declare("installed-dot");
    graph.add_node("source");
    if (graph.to_dot().find("source") == std::string::npos)
    {
        return 1;
    }
    auto pipe = std::make_shared<takt::Pipe<int>>("installed-package");
    pipe->set_consumer({"sink", 1, 1, 0});
    pipe->set_producer({"source", 1, 1}, 0);
    pipe->set_capacity(2);
    takt::PipelineRuntime runtime({}, {pipe});
    runtime.initialize();
    runtime.start();
    {
        auto write = pipe->acquire_configured_write_batch(true);
        write.value(0) = 42;
        write.publish();
    }

    auto read = pipe->acquire_configured_read_batch();
    runtime.join();
    return read.value(0) == 42 ? 0 : 1;
}
