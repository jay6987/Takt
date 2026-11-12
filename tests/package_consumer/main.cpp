// Exercises the installed Takt package through public headers and linkage.
#include <takt/takt.h>

#if __has_include(<takt/pipeline/builder.h>)
#error "The installed package must not expose the removed PipeBuilder header"
#endif
#if __has_include(<takt/pipeline/frame_task_node.h>)
#error "The installed package must not expose FrameTaskNode"
#endif

class InstalledBarrier final : public takt::Node
{
  public:
    // Exercise an empty-group sentinel through the installed Node API.
    InstalledBarrier()
        : Node("installed-barrier",
               2)
    {
    }

    // Join before the derived hook table is released.
    ~InstalledBarrier() override
    {
        request_stop();
        join();
    }

  private:
    // Return one barrier sentinel to complete a group with no work items.
    std::unique_ptr<Task> build_task(uint64_t) override
    {
        if (offered)
            return nullptr;
        offered = true;
        auto task = std::make_unique<Task>();
        task->barrier = true;
        return task;
    }

    // The sentinel has no numerical work to perform.
    void process_task(size_t,
                      Task&) override
    {
    }

    // Observe successful completion after the sentinel is destroyed.
    void on_barrier_complete(uint64_t) override
    {
        ++completed_group_count;
    }

  public:
    // Expose successful completion to the package smoke-test entry point.
    size_t completed_groups() const
    {
        return completed_group_count;
    }

  private:
    bool offered = false;
    size_t completed_group_count = 0;
};

// Verifies linked package behavior using only installed public interfaces.
int main()
{
    InstalledBarrier group;
    group.start();
    group.join();
    if (group.failure() || group.completed_groups() != 1)
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
