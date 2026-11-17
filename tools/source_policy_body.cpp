// Counts effective token lines in callable and function-try bodies.
#include "source_policy.h"

#include "source_contracts_internal.h"

#include <iostream>
#include <set>
#include <stdexcept>

namespace
{
// Adds every source line occupied by a non-comment token, including literals.
void add_token_lines(CXTranslationUnit unit,
                     CXToken token,
                     std::set<unsigned>& lines)
{
    CXFile start_file = nullptr;
    CXFile end_file = nullptr;
    unsigned start_line = 0;
    unsigned end_line = 0;
    const auto range = clang_getTokenExtent(unit, token);
    clang_getExpansionLocation(clang_getRangeStart(range), &start_file,
                               &start_line, nullptr, nullptr);
    clang_getExpansionLocation(clang_getRangeEnd(range), &end_file, &end_line,
                               nullptr, nullptr);
    if (start_file == nullptr || start_file != end_file ||
        end_line < start_line)
        throw std::runtime_error("callable token has unsupported source range");
    for (unsigned line = start_line; line <= end_line; ++line)
        lines.insert(line);
}

// Counts the body range's unique non-comment token lines, including braces.
std::size_t effective_lines(CXTranslationUnit unit,
                            CXCursor body)
{
    CXToken* tokens = nullptr;
    unsigned count = 0;
    clang_tokenize(unit, clang_getCursorExtent(body), &tokens, &count);
    std::set<unsigned> lines;
    for (unsigned index = 0; index < count; ++index)
    {
        if (clang_getTokenKind(tokens[index]) != CXToken_Comment)
            add_token_lines(unit, tokens[index], lines);
    }
    clang_disposeTokens(unit, tokens, count);
    return lines.size();
}

// Applies the body cap once per authored expansion location.
void inspect_body(CXTranslationUnit unit,
                  CXCursor body,
                  source_policy::State& state)
{
    CXFile file = nullptr;
    unsigned line = 0;
    unsigned offset = 0;
    clang_getExpansionLocation(clang_getRangeStart(clang_getCursorExtent(body)),
                               &file, &line, nullptr, &offset);
    if (file == nullptr ||
        !source_policy::is_owned(quality::detail::text(clang_getFileName(file)),
                                 state))
        return;
    const auto path = quality::detail::text(clang_getFileName(file));
    const auto key = path + ":" + std::to_string(offset);
    if (!state.checked_bodies.insert(key).second)
        return;
    const auto lines = effective_lines(unit, body);
    if (state.body_limit != 0 && lines > state.body_limit)
    {
        std::cerr << path << ":" << line << ": callable body has " << lines
                  << " effective non-comment lines; maximum "
                  << state.body_limit << '\n';
        state.failed = true;
    }
}

// Selects direct compound and function-try body cursors.
CXChildVisitResult inspect_body_child(CXCursor child,
                                      CXCursor,
                                      CXClientData data)
{
    const auto kind = clang_getCursorKind(child);
    if (kind == CXCursor_CompoundStmt || kind == CXCursor_CXXTryStmt)
    {
        auto& state = *static_cast<source_policy::State*>(data);
        inspect_body(clang_Cursor_getTranslationUnit(child), child, state);
    }
    return CXChildVisit_Continue;
}
} // namespace

namespace source_policy
{
void inspect_callable_bodies(CXCursor callable,
                             State& state)
{
    clang_visitChildren(callable, inspect_body_child, &state);
}
} // namespace source_policy
