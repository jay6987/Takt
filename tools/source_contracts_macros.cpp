// Validates descriptions attached to authored GoogleTest macro invocations.
#include "source_contracts.h"

#include "source_contracts_internal.h"

#include <iostream>

namespace quality
{
void SourceContracts::inspect_macro(CXCursor cursor)
{
    const auto name = detail::text(clang_getCursorSpelling(cursor));
    if (name != "TEST" && name != "TEST_F" && name != "TEST_P" &&
        name != "TYPED_TEST" && name != "TYPED_TEST_P")
        return;
    CXFile file = nullptr;
    unsigned offset = 0;
    clang_getSpellingLocation(clang_getCursorLocation(cursor), &file, nullptr,
                              nullptr, &offset);
    const auto source =
        detail::read_source(detail::text(clang_getFileName(file)));
    if (!detail::macro_documented(source, offset, detail::header_end(source)))
    {
        std::cerr << detail::location(cursor) << ": undocumented test case\n";
        failed_ = true;
    }
}
} // namespace quality
