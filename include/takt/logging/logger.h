// Exposes replaceable process-wide logging for narrow and wide messages.
#pragma once

#include <memory>
#include <string>

namespace takt
{
/// Interface for process-wide severity-aware and undecorated logging.
class ILogger
{
  public:
    // Allows custom logger implementations to be released polymorphically.
    virtual ~ILogger() = default;

    // Displays a narrow message without severity decoration.
    virtual void display(const std::string& message) = 0;
    // Displays a wide message without severity decoration.
    virtual void display(const std::wstring& message) = 0;
    // Reports an informational narrow message.
    virtual void info(const std::string& message) = 0;
    // Reports an informational wide message.
    virtual void info(const std::wstring& message) = 0;
    // Reports a narrow warning.
    virtual void warn(const std::string& message) = 0;
    // Reports a wide warning.
    virtual void warn(const std::wstring& message) = 0;
    // Reports a narrow error.
    virtual void error(const std::string& message) = 0;
    // Reports a wide error.
    virtual void error(const std::wstring& message) = 0;
    // Reports a narrow debug message according to logger policy.
    virtual void debug(const std::string& message) = 0;
    // Reports a wide debug message according to logger policy.
    virtual void debug(const std::wstring& message) = 0;
    // Reports whether debug output is enabled.
    virtual bool debug_enabled() const = 0;
};

// Returns shared ownership of the currently installed process-wide logger.
std::shared_ptr<ILogger> get_logger();
// Installs a nonnull logger; null leaves the current logger unchanged.
void set_logger(std::shared_ptr<ILogger> logger);
// Restores a fresh default console logger.
void reset_logger();
// Changes debug output on the default logger, if it is still installed.
void set_debug_enabled(bool enabled);

// Displays a narrow message through the current logger.
void display(const std::string& message);
// Displays a wide message through the current logger.
void display(const std::wstring& message);
// Logs a narrow informational message through the current logger.
void info(const std::string& message);
// Logs a wide informational message through the current logger.
void info(const std::wstring& message);
// Logs a narrow warning through the current logger.
void warn(const std::string& message);
// Logs a wide warning through the current logger.
void warn(const std::wstring& message);
// Logs a narrow error through the current logger.
void error(const std::string& message);
// Logs a wide error through the current logger.
void error(const std::wstring& message);
// Logs a narrow debug message through the current logger.
void debug(const std::string& message);
// Logs a wide debug message through the current logger.
void debug(const std::wstring& message);
} // namespace takt
