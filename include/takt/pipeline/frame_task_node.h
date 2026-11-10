// Groups bounded worker tasks into ordered frames with framework-owned drain.
#pragma once

#include "takt/pipeline/node.h"
#include "takt/pipeline/pipe_base.h"

namespace takt
{
class FrameTaskNode : public Node
{
  public:
    struct Frame
    {
        // Releases tokens only after the framework has drained frame tasks.
        virtual ~Frame() = default;
        uint64_t index = 0;
    };

    // Reuses Node's permanent workers and bounded queue for frame-local work.
    FrameTaskNode(std::string name,
                  size_t workers = 1,
                  size_t queue_capacity = 1);
    // Stops and joins; derived classes must join before their state disappears.
    ~FrameTaskNode() override;
    // Stops admission and closes registered pipes to wake acquisition waits.
    void request_stop() noexcept override;
    // Reports frames whose completion hook succeeded, not finished task count.
    size_t completed_frames() const noexcept;

  protected:
    // Registers lifecycle dependencies before start; output closes on normal
    // EOF.
    void register_pipe(std::shared_ptr<PipeBase> pipe,
                       bool output = false);
    // Allocates a context before acquisition so exceptions cannot destroy
    // tokens.
    virtual std::unique_ptr<Frame> create_frame() = 0;
    // Acquires/prepares one frame on ingress; false means normal input EOF.
    virtual bool acquire_frame(Frame& frame) = 0;
    // Notifies start on ingress, before any tasks for this frame are admitted.
    virtual void on_frame_start(Frame& frame);
    // Lazily emits frame-local tasks; null seals the group, including empty
    // ones.
    virtual std::unique_ptr<Task> build_frame_task(Frame& frame,
                                                   uint64_t index) = 0;
    // Processes one item on the indicated permanent worker.
    virtual void process_frame_task(size_t worker,
                                    Frame& frame,
                                    Task& task) = 0;
    // Runs on ingress after task drain; throwing prevents publication/progress.
    virtual void on_frame_complete(Frame& frame);
    // Observes the original error before fault closure or token destruction.
    virtual void on_frame_failure(std::exception_ptr error);

  private:
    struct GroupTask;
    // Advances one sealed frame only after every admitted task has drained.
    std::unique_ptr<Task> build_task(uint64_t sequence) override final;
    // Executes the wrapped item and records failure before cleanup.
    void process_task(size_t worker,
                      Task& task) override final;
    // Frame adapters use group completion instead of per-task completion hooks.
    void on_task_done(size_t,
                      const Task&) override final
    {
    }
    // Drains active tasks before releasing the owning frame and closing
    // outputs.
    void on_ingress_manager_done() override final;
    // Remembers the cause and fault-closes every configured dependency.
    void abort_frame(const std::exception_ptr& error);
    std::unique_ptr<Frame> current_;
    uint64_t next_frame_ = 0, next_task_ = 0;
    std::atomic<size_t> completed_{0};
    std::vector<std::shared_ptr<PipeBase>> pipes_, outputs_;
};
} // namespace takt
