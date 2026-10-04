#include "takt/logging/logger.h"

#include <chrono>
#include <ctime>
#include <cwchar>
#include <iomanip>
#include <iostream>
#include <memory>
#include <mutex>
#include <sstream>
#include <vector>

namespace takt
{
namespace
{
class DefaultLogger final : public ILogger
{
  public:
    void display(const std::string& message) override
    {
        std::lock_guard<std::mutex> lk(mu_);
        std::cout << message << std::endl;
    }

    void display(const std::wstring& message) override
    {
        std::lock_guard<std::mutex> lk(mu_);
        std::wcout << message << std::endl;
    }

    void info(const std::string& message) override
    {
        write("INFO", message);
    }
    void info(const std::wstring& message) override
    {
        write("INFO", to_utf8(message));
    }
    void warn(const std::string& message) override
    {
        write("WARN", message);
    }
    void warn(const std::wstring& message) override
    {
        write("WARN", to_utf8(message));
    }
    void error(const std::string& message) override
    {
        write("ERROR", message);
    }
    void error(const std::wstring& message) override
    {
        write("ERROR", to_utf8(message));
    }
    void debug(const std::string& message) override
    {
        if (debug_enabled_)
        {
            write("DEBUG", message);
        }
    }

    void debug(const std::wstring& message) override
    {
        if (debug_enabled_)
        {
            write("DEBUG", to_utf8(message));
        }
    }

    bool debug_enabled() const override
    {
        return debug_enabled_;
    }

    void set_debug(bool enabled)
    {
        debug_enabled_ = enabled;
    }

  private:
    static std::string now_string()
    {
        const auto now = std::chrono::system_clock::now();
        const auto t = std::chrono::system_clock::to_time_t(now);

        std::tm tm_buf{};
#ifdef _WIN32
        localtime_s(&tm_buf, &t);
#else
        localtime_r(&t, &tm_buf);
#endif

        std::ostringstream oss;
        oss << std::put_time(&tm_buf, "%H:%M:%S");
        return oss.str();
    }

    static std::string to_utf8(const std::wstring& w)
    {
        if (w.empty())
        {
            return {};
        }

        std::mbstate_t state{};
        const wchar_t* src = w.c_str();
        const size_t len = std::wcsrtombs(nullptr, &src, 0, &state);
        if (len == static_cast<size_t>(-1))
        {
            return "[wstring-convert-failed]";
        }

        std::vector<char> buffer(len + 1, '\0');
        state = std::mbstate_t{};
        src = w.c_str();
        std::wcsrtombs(buffer.data(), &src, buffer.size(), &state);
        return std::string(buffer.data());
    }

    void write(const char* level, const std::string& message)
    {
        std::lock_guard<std::mutex> lk(mu_);
        std::cout << "[" << now_string() << "] " << level << " " << message
                  << std::endl;
    }

    std::mutex mu_;
    bool debug_enabled_ = false;
};

std::mutex g_mu;
std::shared_ptr<ILogger> g_logger = std::make_shared<DefaultLogger>();

DefaultLogger* get_default_logger()
{
    return dynamic_cast<DefaultLogger*>(g_logger.get());
}
} // namespace

std::shared_ptr<ILogger> get_logger()
{
    std::lock_guard<std::mutex> lk(g_mu);
    return g_logger;
}

void set_logger(std::shared_ptr<ILogger> logger)
{
    if (!logger)
    {
        return;
    }

    std::lock_guard<std::mutex> lk(g_mu);
    g_logger = std::move(logger);
}

void reset_logger()
{
    std::lock_guard<std::mutex> lk(g_mu);
    g_logger = std::make_shared<DefaultLogger>();
}

void set_debug_enabled(bool enabled)
{
    std::lock_guard<std::mutex> lk(g_mu);
    if (auto* logger = get_default_logger())
    {
        logger->set_debug(enabled);
    }
}

void display(const std::string& message)
{
    get_logger()->display(message);
}
void display(const std::wstring& message)
{
    get_logger()->display(message);
}
void info(const std::string& message)
{
    get_logger()->info(message);
}
void info(const std::wstring& message)
{
    get_logger()->info(message);
}
void warn(const std::string& message)
{
    get_logger()->warn(message);
}
void warn(const std::wstring& message)
{
    get_logger()->warn(message);
}
void error(const std::string& message)
{
    get_logger()->error(message);
}
void error(const std::wstring& message)
{
    get_logger()->error(message);
}
void debug(const std::string& message)
{
    get_logger()->debug(message);
}
void debug(const std::wstring& message)
{
    get_logger()->debug(message);
}
} // namespace takt
