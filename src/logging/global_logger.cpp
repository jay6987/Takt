// Owns and dispatches to the replaceable process-wide logger.
#include "takt/logging/logger.h"

#include "default_logger.h"

#include <memory>
#include <mutex>
#include <utility>

namespace takt
{
namespace
{
std::mutex g_mu;
std::shared_ptr<ILogger> g_logger = std::make_shared<detail::DefaultLogger>();
} // namespace

std::shared_ptr<ILogger> get_logger()
{
    std::lock_guard<std::mutex> lk(g_mu);
    return g_logger;
}

void set_logger(std::shared_ptr<ILogger> logger)
{
    if (!logger)
        return;
    std::lock_guard<std::mutex> lk(g_mu);
    g_logger = std::move(logger);
}

void reset_logger()
{
    std::lock_guard<std::mutex> lk(g_mu);
    g_logger = std::make_shared<detail::DefaultLogger>();
}

void set_debug_enabled(bool enabled)
{
    std::lock_guard<std::mutex> lk(g_mu);
    if (auto* logger = dynamic_cast<detail::DefaultLogger*>(g_logger.get()))
        logger->set_debug(enabled);
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
