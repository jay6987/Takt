// Applies strict class and callable documentation checks to parsed ASTs.
#include "source_policy.h"

#include "source_contracts_internal.h"

namespace
{
// Recognizes callable AST nodes, including lambdas and function templates.
bool is_callable(CXCursorKind kind)
{
    return kind == CXCursor_FunctionDecl || kind == CXCursor_CXXMethod ||
           kind == CXCursor_Constructor || kind == CXCursor_Destructor ||
           kind == CXCursor_ConversionFunction ||
           kind == CXCursor_FunctionTemplate || kind == CXCursor_LambdaExpr ||
           kind == CXCursor_ObjCInstanceMethodDecl ||
           kind == CXCursor_ObjCClassMethodDecl;
}

// Visits only owned subtrees and records classes, functions and test macros.
CXChildVisitResult inspect_declaration(CXCursor cursor,
                                       CXCursor,
                                       CXClientData data)
{
    auto& state = *static_cast<source_policy::State*>(data);
    CXFile expansion_file = nullptr;
    clang_getExpansionLocation(clang_getCursorLocation(cursor), &expansion_file,
                               nullptr, nullptr, nullptr);
    if (expansion_file != nullptr &&
        !source_policy::is_owned(
            quality::detail::text(clang_getFileName(expansion_file)), state))
        return CXChildVisit_Continue;

    CXFile spelling_file = nullptr;
    clang_getSpellingLocation(clang_getCursorLocation(cursor), &spelling_file,
                              nullptr, nullptr, nullptr);
    const bool authored =
        spelling_file != nullptr &&
        source_policy::is_owned(
            quality::detail::text(clang_getFileName(spelling_file)), state);
    const auto kind = clang_getCursorKind(cursor);
    if (!state.contracts_only && authored && kind == CXCursor_MacroExpansion)
        state.contracts.inspect_macro(cursor);
    const bool type = kind == CXCursor_ClassDecl ||
                      kind == CXCursor_StructDecl ||
                      kind == CXCursor_ClassTemplate ||
                      kind == CXCursor_ClassTemplatePartialSpecialization;
    if (!state.contracts_only && authored && type &&
        clang_isCursorDefinition(cursor))
        state.contracts.inspect_type(cursor);
    if (is_callable(kind))
    {
        if (authored && kind != CXCursor_LambdaExpr)
            state.contracts.inspect_function(cursor);
        if (!state.contracts_only)
            source_policy::inspect_callable_bodies(cursor, state);
    }
    return CXChildVisit_Recurse;
}
} // namespace

namespace source_policy
{
State::State(std::filesystem::path repository_root)
    : root(std::filesystem::absolute(repository_root).lexically_normal())
{
}

bool is_owned(const std::filesystem::path& path,
              const State& state)
{
    const auto absolute =
        path.is_absolute() ? path.lexically_normal()
                           : std::filesystem::absolute(path).lexically_normal();
    const auto relative = absolute.lexically_relative(state.root);
    const auto first = relative.begin();
    return first != relative.end() &&
           (*first == "modules" || *first == "apps" || *first == "tools" ||
            *first == "benchmarks" || *first == "tests" ||
            *first == "include" || *first == "src" || *first == "examples");
}

void inspect_translation_unit(CXTranslationUnit unit,
                              State& state)
{
    clang_visitChildren(clang_getTranslationUnitCursor(unit),
                        inspect_declaration, &state);
}
} // namespace source_policy
