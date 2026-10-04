#pragma once

#include <memory>
#include <string>

namespace takt
{
class ILogger
{
  public:
    virtual ~ILogger() = default;

    virtual void display(const std::string& message) = 0;
    virtual void display(const std::wstring& message) = 0;
    virtual void info(const std::string& message) = 0;
    virtual void info(const std::wstring& message) = 0;
    virtual void warn(const std::string& message) = 0;
    virtual void warn(const std::wstring& message) = 0;
    virtual void error(const std::string& message) = 0;
    virtual void error(const std::wstring& message) = 0;
    virtual void debug(const std::string& message) = 0;
    virtual void debug(const std::wstring& message) = 0;
    virtual bool debug_enabled() const = 0;
};

std::shared_ptr<ILogger> get_logger();
void set_logger(std::shared_ptr<ILogger> logger);
void reset_logger();
void set_debug_enabled(bool enabled);

void display(const std::string& message);
void display(const std::wstring& message);
void info(const std::string& message);
void info(const std::wstring& message);
void warn(const std::string& message);
void warn(const std::wstring& message);
void error(const std::string& message);
void error(const std::wstring& message);
void debug(const std::string& message);
void debug(const std::wstring& message);
} // namespace takt
