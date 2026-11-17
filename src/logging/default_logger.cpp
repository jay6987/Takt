// Implements the synchronized default console logger.
#include "default_logger.h"

#include <iostream>

namespace takt::detail
{
void DefaultLogger::display(const std::string& message)
{
    std::lock_guard<std::mutex> lk(mu_);
    std::cout << message << std::endl;
}

void DefaultLogger::display(const std::wstring& message)
{
    std::lock_guard<std::mutex> lk(mu_);
    std::wcout << message << std::endl;
}

void DefaultLogger::info(const std::string& message)
{
    write("INFO", message);
}
void DefaultLogger::info(const std::wstring& message)
{
    write("INFO", to_utf8(message));
}
void DefaultLogger::warn(const std::string& message)
{
    write("WARN", message);
}
void DefaultLogger::warn(const std::wstring& message)
{
    write("WARN", to_utf8(message));
}
void DefaultLogger::error(const std::string& message)
{
    write("ERROR", message);
}
void DefaultLogger::error(const std::wstring& message)
{
    write("ERROR", to_utf8(message));
}
void DefaultLogger::debug(const std::string& message)
{
    if (debug_enabled_)
        write("DEBUG", message);
}
void DefaultLogger::debug(const std::wstring& message)
{
    if (debug_enabled_)
        write("DEBUG", to_utf8(message));
}
bool DefaultLogger::debug_enabled() const
{
    return debug_enabled_;
}
void DefaultLogger::set_debug(bool enabled)
{
    debug_enabled_ = enabled;
}

void DefaultLogger::write(const char* level,
                          const std::string& message)
{
    std::lock_guard<std::mutex> lk(mu_);
    std::cout << "[" << now_string() << "] " << level << " " << message
              << std::endl;
}
} // namespace takt::detail
