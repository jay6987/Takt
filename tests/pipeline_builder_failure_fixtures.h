// Supplies draining nodes and captured logging for builder failure tests.
#pragma once
#include "pipeline_builder_tests_test_support_common.h"
namespace
{

/// Represents the drain string node test fixture.
class DrainStringNode final : public takt::Node
{
  public:
    // Binds a string input used to test incompatible bridge payloads.
    explicit DrainStringNode(std::shared_ptr<takt::Pipe<std::string>> in)
        : takt::Node("drain-string-node"),
          in_(std::move(in))
    {
    }

  protected:
    /// Represents the worker task test fixture.
    struct WorkerTask final : public takt::Node::Task
    {
    };

    // Admits one persistent processing task per configured worker.
    std::unique_ptr<takt::Node::Task> build_task(uint64_t sequence_id) override
    {
        if (sequence_id >= worker_count())
        {
            return nullptr;
        }
        return std::make_unique<WorkerTask>();
    }

    // Drains string input until its pipe signals normal completion.
    void process_task(size_t,
                      takt::Node::Task&) override
    {
        while (true)
        {
            try
            {
                auto r = in_->acquire_read();
                (void)r.value();
            }
            catch (const takt::PipeClosedAndEmptySignal&)
            {
                break;
            }
        }
    }

  private:
    std::shared_ptr<takt::Pipe<std::string>> in_;
};

/// Represents the capturing logger test fixture.
class CapturingLogger final : public takt::ILogger
{
  public:
    // Discards undecorated narrow output during failure assertions.
    void display(const std::string&) override {}
    // Discards undecorated wide output during failure assertions.
    void display(const std::wstring&) override {}
    // Discards narrow informational output.
    void info(const std::string&) override {}
    // Discards wide informational output.
    void info(const std::wstring&) override {}
    // Discards narrow warning output.
    void warn(const std::string&) override {}
    // Discards wide warning output.
    void warn(const std::wstring&) override {}
    // Stores narrow errors under synchronization for later assertions.
    void error(const std::string& message) override
    {
        std::lock_guard<std::mutex> lk(mu_);
        error_messages_.push_back(message);
    }
    // Discards wide errors unused by the structured failure logger.
    void error(const std::wstring&) override {}
    // Discards narrow debug output.
    void debug(const std::string&) override {}
    // Discards wide debug output.
    void debug(const std::wstring&) override {}
    // Keeps debug disabled for deterministic failure assertions.
    bool debug_enabled() const override
    {
        return false;
    }

    // Searches captured error messages while holding their synchronization
    // lock.
    bool has_error_substring(const std::string& needle) const
    {
        std::lock_guard<std::mutex> lk(mu_);
        for (const auto& message : error_messages_)
        {
            if (message.find(needle) != std::string::npos)
            {
                return true;
            }
        }
        return false;
    }

  private:
    mutable std::mutex mu_;
    std::vector<std::string> error_messages_;
};

/// Represents the scoped logger reset test fixture.
class ScopedLoggerReset final
{
  public:
    // Restores default logging even when a test exits through a failed
    // assertion.
    ~ScopedLoggerReset()
    {
        takt::reset_logger();
    }
};
} // namespace
