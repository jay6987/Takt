// Formats timestamps and converts wide messages for console logging.
#include "default_logger.h"

#include <chrono>
#include <ctime>
#include <cwchar>
#include <iomanip>
#include <sstream>
#include <vector>

#ifdef _WIN32
#include <windows.h>
#endif

namespace takt::detail
{
std::string DefaultLogger::now_string()
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

std::string DefaultLogger::to_utf8(const std::wstring& w)
{
    if (w.empty())
        return {};
#ifdef _WIN32
    const int len =
        WideCharToMultiByte(CP_UTF8, 0, w.c_str(), static_cast<int>(w.size()),
                            nullptr, 0, nullptr, nullptr);
    if (len <= 0)
        return "[wstring-convert-failed]";
    std::string buffer(static_cast<size_t>(len), '\0');
    const int written =
        WideCharToMultiByte(CP_UTF8, 0, w.c_str(), static_cast<int>(w.size()),
                            buffer.data(), len, nullptr, nullptr);
    if (written <= 0)
        return "[wstring-convert-failed]";
    return buffer;
#else
    std::mbstate_t state{};
    const wchar_t* src = w.c_str();
    const size_t len = std::wcsrtombs(nullptr, &src, 0, &state);
    if (len == static_cast<size_t>(-1))
        return "[wstring-convert-failed]";
    std::vector<char> buffer(len + 1, '\0');
    state = std::mbstate_t{};
    src = w.c_str();
    std::wcsrtombs(buffer.data(), &src, buffer.size(), &state);
    return std::string(buffer.data());
#endif
}
} // namespace takt::detail
