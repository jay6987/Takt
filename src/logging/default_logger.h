// Declares the synchronized default console logger.
#pragma once

#include "takt/logging/logger.h"

#include <mutex>
#include <string>

namespace takt::detail
{
/// Default synchronized console sink used until an application installs a
/// logger.
class DefaultLogger final : public ILogger
{
  public:
    // Displays undecorated narrow text.
    void display(const std::string& message) override;
    // Displays undecorated wide text.
    void display(const std::wstring& message) override;
    // Logs narrow informational text.
    void info(const std::string& message) override;
    // Converts and logs wide informational text.
    void info(const std::wstring& message) override;
    // Logs a narrow warning.
    void warn(const std::string& message) override;
    // Converts and logs a wide warning.
    void warn(const std::wstring& message) override;
    // Logs a narrow error.
    void error(const std::string& message) override;
    // Converts and logs a wide error.
    void error(const std::wstring& message) override;
    // Logs narrow debug text when enabled.
    void debug(const std::string& message) override;
    // Converts and logs wide debug text when enabled.
    void debug(const std::wstring& message) override;
    // Returns the current debug policy.
    bool debug_enabled() const override;
    // Changes the debug policy.
    void set_debug(bool enabled);

  private:
    // Formats the current local time.
    static std::string now_string();
    // Converts a wide message using the platform policy.
    static std::string to_utf8(const std::wstring& message);
    // Serializes a decorated message.
    void write(const char* level,
               const std::string& message);

    std::mutex mu_;
    bool debug_enabled_ = false;
};
} // namespace takt::detail
