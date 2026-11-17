// Declares implementation helpers shared by the source-contract units.
#pragma once

#include <clang-c/Index.h>

#include <filesystem>
#include <string>
#include <vector>

namespace quality::detail
{
/// Copies a libclang string and releases its allocated storage.
std::string text(CXString value);

/// Reads source bytes and reports failures instead of returning empty text.
std::string read_source(const std::filesystem::path& path);

/// Recognizes comments containing a substantive alphabetic description.
bool descriptive(const std::string& comment);

/// Finds the end of the leading file responsibility comment.
std::size_t header_end(const std::string& source);

/// Returns a cursor's authored spelling location for diagnostics.
std::string location(CXCursor cursor);

/// Checks an immediately preceding descriptive comment on a test macro.
bool macro_documented(const std::string& source,
                      std::size_t start,
                      std::size_t header);

/// Collects direct parameters without descending into function-pointer types.
std::vector<CXCursor> parameters(CXCursor cursor);
} // namespace quality::detail
