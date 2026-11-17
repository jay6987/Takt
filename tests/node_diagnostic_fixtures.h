// Supplies captured logging and ordered character storage for node tests.
#pragma once
#include "node_tests_test_support_common.h"
namespace
{

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

/// Represents the indexed char test fixture.
struct IndexedChar
{
    size_t index = 0;
    char value = '\0';
};

/// Represents the ordered char store test fixture.
class OrderedCharStore
{
  public:
    // Stores an indexed character and wakes ordered consumers.
    void put(size_t idx,
             char c)
    {
        std::lock_guard<std::mutex> lk(mu_);
        store_[idx] = c;
        cv_.notify_all();
    }

    // Marks ingress completion and wakes consumers waiting for missing indices.
    void mark_done()
    {
        std::lock_guard<std::mutex> lk(mu_);
        done_ = true;
        cv_.notify_all();
    }

    // Waits for the requested character or confirmed completion without it.
    bool wait_pop(size_t idx,
                  char& out)
    {
        std::unique_lock<std::mutex> lk(mu_);
        cv_.wait(lk, [&] { return done_ || store_.find(idx) != store_.end(); });

        auto it = store_.find(idx);
        if (it != store_.end())
        {
            out = it->second;
            store_.erase(it);
            return true;
        }
        return false;
    }

  private:
    std::mutex mu_;
    std::condition_variable cv_;
    std::map<size_t, char> store_;
    bool done_ = false;
};
} // namespace
