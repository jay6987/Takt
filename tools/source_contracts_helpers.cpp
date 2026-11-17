// Implements parsing helpers shared by callable and type contract checks.
#include "source_contracts_internal.h"

#include <fstream>
#include <iterator>
#include <stdexcept>

namespace quality::detail
{
std::string text(CXString value)
{
    const char* chars = clang_getCString(value);
    std::string result = chars == nullptr ? "" : chars;
    clang_disposeString(value);
    return result;
}

std::string read_source(const std::filesystem::path& path)
{
    std::ifstream input(path, std::ios::binary);
    if (!input)
        throw std::runtime_error("cannot read source: " + path.string());
    return {std::istreambuf_iterator<char>(input),
            std::istreambuf_iterator<char>()};
}

bool descriptive(const std::string& comment)
{
    for (const char value : comment)
    {
        if ((value >= 'A' && value <= 'Z') || (value >= 'a' && value <= 'z'))
            return true;
    }
    return false;
}

std::size_t header_end(const std::string& source)
{
    const auto start = source.find_first_not_of(" \t\r\n");
    if (start == std::string::npos)
        return 0;
    if (source.compare(start, 2, "/*") == 0)
    {
        const auto end = source.find("*/", start + 2);
        return end == std::string::npos ? 0 : end + 2;
    }
    if (source.compare(start, 2, "//") != 0)
        return 0;
    auto end = source.find('\n', start);
    while (end != std::string::npos)
    {
        const auto next = source.find_first_not_of(" \t\r", end + 1);
        if (next == std::string::npos || source.compare(next, 2, "//") != 0)
            return end;
        end = source.find('\n', next);
    }
    return source.size();
}

std::string location(CXCursor cursor)
{
    CXFile file = nullptr;
    unsigned line = 0;
    clang_getSpellingLocation(clang_getCursorLocation(cursor), &file, &line,
                              nullptr, nullptr);
    return text(clang_getFileName(file)) + ":" + std::to_string(line);
}

bool macro_documented(const std::string& source,
                      std::size_t start,
                      std::size_t header)
{
    if (start == 0)
        return false;
    const auto end = source.find_last_not_of(" \t\r\n", start - 1);
    if (end == std::string::npos || end < header)
        return false;
    if (end > 0 && source.compare(end - 1, 2, "*/") == 0)
    {
        const auto begin = source.rfind("/*", end - 1);
        return begin != std::string::npos &&
               descriptive(source.substr(begin, end + 1 - begin));
    }
    const auto newline = source.rfind('\n', end);
    const auto begin = newline == std::string::npos ? 0 : newline + 1;
    const auto comment = source.substr(begin, end + 1 - begin);
    const auto first = comment.find_first_not_of(" \t");
    return first != std::string::npos && comment.compare(first, 2, "//") == 0 &&
           descriptive(comment.substr(first + 2));
}

std::vector<CXCursor> parameters(CXCursor cursor)
{
    std::vector<CXCursor> result;
    clang_visitChildren(
        cursor,
        [](CXCursor child, CXCursor, CXClientData data)
        {
            if (clang_getCursorKind(child) == CXCursor_ParmDecl)
                static_cast<std::vector<CXCursor>*>(data)->push_back(child);
            return CXChildVisit_Continue;
        },
        &result);
    return result;
}
} // namespace quality::detail
