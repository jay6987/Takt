// Verifies default logger dispatch, wide conversion and debug-output policy.
#include "takt/takt.h"

#include <gtest/gtest.h>

#include <iostream>
#include <sstream>
#include <streambuf>

namespace
{
/// Captures console output while restoring the process-wide logger.
class ConsoleCapture
{
  public:
    // Redirects both console streams while preserving their original buffers.
    ConsoleCapture()
        : narrow_buffer_(std::cout.rdbuf(narrow.rdbuf())),
          wide_buffer_(std::wcout.rdbuf(wide.rdbuf()))
    {
        takt::reset_logger();
    }

    // Restores console buffers and process-wide logging even after assertions.
    ~ConsoleCapture()
    {
        takt::reset_logger();
        std::cout.rdbuf(narrow_buffer_);
        std::wcout.rdbuf(wide_buffer_);
    }

    // Stream-buffer ownership must remain with a single scope.
    ConsoleCapture(const ConsoleCapture&) = delete;
    // Prevents copying console restoration responsibility.
    ConsoleCapture& operator=(const ConsoleCapture&) = delete;

    std::ostringstream narrow;
    std::wostringstream wide;

  private:
    std::streambuf* narrow_buffer_;
    std::wstreambuf* wide_buffer_;
};

// Confirms reset restores an independent default logger with debug disabled.
void verify_logger_reset(const std::shared_ptr<takt::ILogger>& original)
{
    takt::reset_logger();
    EXPECT_NE(takt::get_logger(), original);
    EXPECT_FALSE(takt::get_logger()->debug_enabled());
}
} // namespace

// Verifies default narrow/wide dispatch and debug enable/disable transitions.
TEST(Logging,
     DefaultDispatchAndDebugPolicy)
{
    ConsoleCapture capture;
    const auto original = takt::get_logger();
    takt::set_logger(nullptr);
    EXPECT_EQ(takt::get_logger(), original);
    EXPECT_FALSE(original->debug_enabled());

    takt::display("plain");
    takt::display(L"wide-plain");
    takt::info("narrow-info");
    takt::info(L"wide-info");
    takt::warn("narrow-warning");
    takt::warn(L"wide-warning");
    takt::error("narrow-error");
    takt::error(L"wide-error");
    takt::info(L"");
    takt::debug("hidden-narrow");
    takt::debug(L"hidden-wide");
    EXPECT_EQ(capture.narrow.str().find("hidden"), std::string::npos);

    takt::set_debug_enabled(true);
    EXPECT_TRUE(original->debug_enabled());
    takt::debug("visible-narrow");
    takt::debug(L"visible-wide");
    const auto output = capture.narrow.str();
    EXPECT_NE(output.find("plain\n"), std::string::npos);
    EXPECT_EQ(capture.wide.str(), L"wide-plain\n");
    for (const auto* message :
         {"narrow-info", "wide-info", "narrow-warning", "wide-warning",
          "narrow-error", "wide-error", "visible-narrow", "visible-wide"})
        EXPECT_NE(output.find(message), std::string::npos) << message;
    EXPECT_NE(output.find("INFO"), std::string::npos);
    EXPECT_NE(output.find("WARN"), std::string::npos);
    EXPECT_NE(output.find("ERROR"), std::string::npos);
    EXPECT_NE(output.find("DEBUG"), std::string::npos);

    takt::set_debug_enabled(false);
    EXPECT_FALSE(original->debug_enabled());
    takt::debug("hidden-again");
    EXPECT_EQ(capture.narrow.str(), output);
    verify_logger_reset(original);
}
