// Enforces hard line widths, file descriptions and callable source contracts.
#include "source_contracts.h"

#include <fstream>
#include <iostream>
#include <iterator>
#include <regex>
#include <stdexcept>
#include <vector>

namespace quality
{
namespace
{
// Releases libclang strings immediately after copying their contents.
std::string text(CXString value)
{
    const char* chars = clang_getCString(value);
    std::string result = chars == nullptr ? "" : chars;
    clang_disposeString(value);
    return result;
}

// Reads source bytes with explicit failure rather than an empty-file fallback.
std::string read(const std::filesystem::path& path)
{
    std::ifstream input(path, std::ios::binary);
    if (!input)
        throw std::runtime_error("cannot read source: " + path.string());
    return {std::istreambuf_iterator<char>(input),
            std::istreambuf_iterator<char>()};
}

// Recognizes substantive comments; punctuation-only markers are not
// descriptions.
bool descriptive(const std::string& comment)
{
    return std::regex_search(comment, std::regex("[A-Za-z]"));
}

// Finds the end of the leading responsibility comment, excluding later
// comments.
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

// Checks an immediately preceding descriptive comment on authored test macros.
bool macro_documented(const std::string& source,
                      std::size_t start,
                      std::size_t header)
{
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

// Uses spelling locations so diagnostics identify authored source, not vendors.
std::string location(CXCursor cursor)
{
    CXFile file = nullptr;
    unsigned line = 0;
    clang_getSpellingLocation(clang_getCursorLocation(cursor), &file, &line,
                              nullptr, nullptr);
    return text(clang_getFileName(file)) + ":" + std::to_string(line);
}

// Detects GoogleTest-generated callables at their macro invocation location.
bool generated_test(CXCursor cursor)
{
    CXFile file = nullptr;
    unsigned offset = 0;
    clang_getSpellingLocation(clang_getCursorLocation(cursor), &file, nullptr,
                              nullptr, &offset);
    const auto source = read(text(clang_getFileName(file)));
    return std::regex_search(
        source.substr(offset),
        std::regex("^(TEST|TEST_F|TEST_P|TYPED_TEST|TYPED_TEST_P)\\s*\\("));
}

// Collects only direct parameters, excluding nested function-pointer
// signatures.
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
} // namespace

void SourceContracts::inspect_file(const std::filesystem::path& path)
{
    if (!files_.insert(path).second)
        return;
    const auto source = read(path);
    const auto header = header_end(source);
    if (header == 0 || !descriptive(source.substr(0, header)))
    {
        std::cerr << path << ": missing file responsibility comment\n";
        failed_ = true;
    }
    std::size_t column = 0;
    unsigned line = 1;
    for (const char value : source)
    {
        if (value == '\n')
        {
            column = 0;
            ++line;
            continue;
        }
        if (value == '\r')
            continue;
        column += value == '\t' ? 4 - column % 4 : 1;
        if (column > 80)
        {
            std::cerr << path << ':' << line << ": line exceeds 80 columns\n";
            failed_ = true;
            column = 0;
        }
    }
}

void SourceContracts::inspect_macro(CXCursor cursor)
{
    const auto name = text(clang_getCursorSpelling(cursor));
    if (name != "TEST" && name != "TEST_F" && name != "TEST_P" &&
        name != "TYPED_TEST" && name != "TYPED_TEST_P")
        return;
    CXFile file = nullptr;
    unsigned offset = 0;
    clang_getSpellingLocation(clang_getCursorLocation(cursor), &file, nullptr,
                              nullptr, &offset);
    const auto source = read(text(clang_getFileName(file)));
    if (offset == 0 || !macro_documented(source, offset, header_end(source)))
    {
        std::cerr << location(cursor) << ": undocumented test case\n";
        failed_ = true;
    }
}

bool SourceContracts::documented(CXCursor cursor) const
{
    const auto comment = text(clang_Cursor_getRawCommentText(cursor));
    if (!descriptive(comment))
        return false;
    CXFile file = nullptr;
    unsigned offset = 0;
    clang_getSpellingLocation(
        clang_getRangeStart(clang_Cursor_getCommentRange(cursor)), &file,
        nullptr, nullptr, &offset);
    return file != nullptr &&
           offset >= header_end(read(text(clang_getFileName(file))));
}

void SourceContracts::inspect_function(CXCursor cursor)
{
    if (generated_test(cursor))
        return;
    auto key = text(clang_getCursorUSR(cursor));
    if (key.empty())
        key = location(cursor);
    if (text(clang_getCursorSpelling(cursor)) == "main")
    {
        CXFile file = nullptr;
        clang_getSpellingLocation(clang_getCursorLocation(cursor), &file,
                                  nullptr, nullptr, nullptr);
        key += ":" + text(clang_getFileName(file));
    }
    auto& contract = functions_[key];
    contract.location = location(cursor);
    contract.documented |=
        documented(cursor) || documented(clang_getCanonicalCursor(cursor));
    unsigned previous_end = 0;
    for (const auto parameter : parameters(cursor))
    {
        unsigned first = 0;
        unsigned last = 0;
        const auto range = clang_getCursorExtent(parameter);
        clang_getSpellingLocation(clang_getRangeStart(range), nullptr, &first,
                                  nullptr, nullptr);
        clang_getSpellingLocation(clang_getRangeEnd(range), nullptr, &last,
                                  nullptr, nullptr);
        if (previous_end >= first)
        {
            std::cerr << location(cursor)
                      << ": parameters must occupy separate lines\n";
            failed_ = true;
        }
        previous_end = last;
    }
}

void SourceContracts::parsed_file(const std::filesystem::path& path)
{
    parsed_.insert(path.lexically_normal());
}

bool SourceContracts::finish(bool require_complete)
{
    if (require_complete)
    {
        for (const auto& file : files_)
        {
            if (!parsed_.contains(file.lexically_normal()))
            {
                std::cerr << file << ": source not reached by the build AST\n";
                failed_ = true;
            }
        }
    }
    for (const auto& [key, contract] : functions_)
    {
        (void)key;
        if (!contract.documented)
        {
            std::cerr << contract.location << ": missing function comment\n";
            failed_ = true;
        }
    }
    return !failed_;
}
} // namespace quality
