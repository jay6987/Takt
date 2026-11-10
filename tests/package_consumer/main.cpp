// Exercises the installed Takt package through public headers and linkage.
#include <takt/takt.h>

#if __has_include(<takt/pipeline/builder.h>)
#error "The installed package must not expose the removed PipeBuilder header"
#endif

class InstalledGroup final : public takt::FrameTaskNode
{
  public:
    // Check linked grouped scheduling through installed C++17 headers.
    InstalledGroup()
        : FrameTaskNode("installed-group",
                        2)
    {
    }
    // Join before the derived hook table is released.
    ~InstalledGroup() override
    {
        request_stop();
        join();
    }

  private:
    // Use an empty frame to exercise explicit zero-task completion.
    std::unique_ptr<Frame> create_frame() override
    {
        return std::make_unique<Frame>();
    }
    // Admit exactly one frame.
    bool acquire_frame(Frame& frame) override
    {
        return frame.index == 0;
    }
    // Seal without numerical work.
    std::unique_ptr<Task> build_frame_task(Frame&,
                                           uint64_t) override
    {
        return nullptr;
    }
    // Empty groups never invoke workers.
    void process_frame_task(size_t,
                            Frame&,
                            Task&) override
    {
    }
};

// Verifies linked package behavior using only installed public interfaces.
int main()
{
    InstalledGroup group;
    group.start();
    group.join();
    if (group.failure() || group.completed_frames() != 1)
        return 2;
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
