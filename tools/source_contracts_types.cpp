// Requires substantive Doxygen descriptions on every defined owned type.
#include "source_contracts.h"

#include "source_contracts_internal.h"

namespace
{
// Accepts the Doxygen line and block comment forms with actual prose.
bool doxygen_documented(CXCursor cursor)
{
    const auto comment =
        quality::detail::text(clang_Cursor_getRawCommentText(cursor));
    const bool doxygen =
        comment.starts_with("///") || comment.starts_with("//!") ||
        comment.starts_with("/**") || comment.starts_with("/*!");
    if (!doxygen || !quality::detail::descriptive(comment))
        return false;

    CXFile file = nullptr;
    unsigned offset = 0;
    clang_getSpellingLocation(
        clang_getRangeStart(clang_Cursor_getCommentRange(cursor)), &file,
        nullptr, nullptr, &offset);
    if (file == nullptr)
        return false;
    const auto source = quality::detail::read_source(
        quality::detail::text(clang_getFileName(file)));
    return offset >= quality::detail::header_end(source);
}
} // namespace

namespace quality
{
void SourceContracts::inspect_type(CXCursor cursor)
{
    CXFile file = nullptr;
    unsigned line = 0;
    unsigned column = 0;
    unsigned offset = 0;
    clang_getSpellingLocation(clang_getCursorLocation(cursor), &file, &line,
                              &column, &offset);
    const auto path = detail::text(clang_getFileName(file));
    const auto key = path + ":" + std::to_string(offset);
    auto& contract = types_[key];
    contract.location =
        path + ":" + std::to_string(line) + ":" + std::to_string(column);
    const auto canonical = clang_getCanonicalCursor(cursor);
    contract.documented |=
        doxygen_documented(cursor) || doxygen_documented(canonical);
}
} // namespace quality
