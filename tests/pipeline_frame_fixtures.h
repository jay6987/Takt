// Supplies frame copy accounting and token-owning vector processing probes.
#pragma once
#include "pipeline_contract_tests_test_support_common.h"
namespace
{
using namespace std::chrono_literals;
using Frame = std::vector<float>;
using FramePipe = takt::Pipe<Frame>;

/// Represents the copy probe test fixture.
struct CopyProbe
{
    static inline int copies = 0;
    // Creates a payload whose subsequent copies are counted.
    CopyProbe() = default;
    // Counts construction copies to detect unnecessary frame duplication.
    CopyProbe(const CopyProbe&)
    {
        ++copies;
    }
    // Counts assignment copies to detect unnecessary payload duplication.
    CopyProbe& operator=(const CopyProbe&)
    {
        ++copies;
        return *this;
    }
};

/// Represents the frame node test fixture.
class FrameNode final : public takt::Node
{
  public:
    // Binds vector frame pipes with optional deterministic worker failure.
    FrameNode(FramePipe& input,
              FramePipe& output,
              bool inject_failure)
        : Node("frame"),
          input_(input),
          output_(output),
          inject_failure_(inject_failure)
    {
    }
    // Stops and joins workers before referenced pipe members are destroyed.
    ~FrameNode() override
    {
        request_stop();
        input_.close();
        output_.close();
        join();
    }
    std::atomic<bool> storage_preserved{true};

  private:
    /// Represents the frame task test fixture.
    struct FrameTask final : Task
    {
        // Retains input/output tokens and the original input storage address.
        FrameTask(FramePipe::ReadToken input_token,
                  FramePipe::WriteToken output_token)
            : input(std::move(input_token)),
              output(std::move(output_token)),
              reserved_address(input.value().data())
        {
        }
        FramePipe::ReadToken input;
        FramePipe::WriteToken output;
        const float* reserved_address;
    };
    // Reserves token-owned frames until the input stream is drained.
    std::unique_ptr<Task> build_task(uint64_t) override
    {
        try
        {
            auto input = input_.acquire_read();
            auto output = input.is_shot_end()
                              ? output_.acquire_shot_end_write(1)
                              : output_.acquire_write();
            return std::make_unique<FrameTask>(std::move(input),
                                               std::move(output));
        }
        catch (const takt::PipeClosedAndEmptySignal&)
        {
            return nullptr;
        }
    }
    // Doubles vector elements and checks token-owned input storage stability.
    void process_task(size_t,
                      Task& task) override
    {
        auto& frame = static_cast<FrameTask&>(task);
        if (inject_failure_ && task.sequence_id == 2)
        {
            throw std::runtime_error("injected frame failure");
        }
        storage_preserved.store(storage_preserved.load() &&
                                frame.input.value().data() ==
                                    frame.reserved_address);
        for (size_t i = 0; i < frame.input.value().size(); ++i)
        {
            frame.output.value()[i] = frame.input.value()[i] * 2.0F;
        }
        frame.output.publish();
    }
    // Closes output after no further frames can be admitted.
    void on_ingress_manager_done() override
    {
        output_.close();
    }
    // Faults both pipe boundaries and preserves standard failure logging.
    void on_exception(const takt::NodeExceptionContext& context) override
    {
        input_.close_with_error(context.message);
        output_.close_with_error(context.message);
        Node::on_exception(context);
    }
    FramePipe& input_;
    FramePipe& output_;
    bool inject_failure_;
};
} // namespace
