// Parses compilation commands and rejects errors before accepting source ASTs.
#include "source_policy.h"

#include "source_contracts_internal.h"

#include <iostream>
#include <vector>

namespace
{
// Retains compiler inputs and target flags while removing output arguments.
std::vector<std::string> compiler_arguments(CXCompileCommand command)
{
    std::vector<std::string> storage;
    const auto count = clang_CompileCommand_getNumArgs(command);
    storage.reserve(count + 2);
    for (unsigned index = 0; index < count; ++index)
    {
        const auto value =
            quality::detail::text(clang_CompileCommand_getArg(command, index));
        if (value == "-c")
            continue;
        if (value == "-o" || value == "-MF" || value == "-MT" ||
            value == "-MQ" || value == "-MJ")
        {
            ++index;
            continue;
        }
        storage.push_back(value);
    }
    storage.push_back(
        "-working-directory=" +
        quality::detail::text(clang_CompileCommand_getDirectory(command)));
    storage.emplace_back("-fparse-all-comments");
    return storage;
}

// Reports all compiler errors rather than auditing an invalid translation unit.
bool diagnostics_valid(CXTranslationUnit unit)
{
    bool valid = true;
    const auto count = clang_getNumDiagnostics(unit);
    for (unsigned index = 0; index < count; ++index)
    {
        CXDiagnostic diagnostic = clang_getDiagnostic(unit, index);
        if (clang_getDiagnosticSeverity(diagnostic) >= CXDiagnostic_Error)
        {
            std::cerr << quality::detail::text(clang_formatDiagnostic(
                             diagnostic,
                             clang_defaultDiagnosticDisplayOptions()))
                      << '\n';
            valid = false;
        }
        clang_disposeDiagnostic(diagnostic);
    }
    return valid;
}

// Records the main source and every included file for completeness checking.
void record_reachable(CXTranslationUnit unit,
                      CXCompileCommand command,
                      source_policy::State& state)
{
    auto file = std::filesystem::path(
        quality::detail::text(clang_CompileCommand_getFilename(command)));
    if (!file.is_absolute())
        file =
            quality::detail::text(clang_CompileCommand_getDirectory(command)) /
            file;
    state.contracts.parsed_file(file);
    clang_getInclusions(
        unit,
        [](CXFile included, CXSourceLocation*, unsigned, CXClientData data)
        {
            auto& audit = *static_cast<source_policy::State*>(data);
            audit.contracts.parsed_file(
                quality::detail::text(clang_getFileName(included)));
        },
        &state);
}
} // namespace

namespace source_policy
{
bool inspect_command(CXIndex index,
                     CXCompileCommand command,
                     State& state)
{
    const auto storage = compiler_arguments(command);
    std::vector<const char*> arguments;
    arguments.reserve(storage.size());
    for (const auto& argument : storage)
        arguments.push_back(argument.c_str());

    CXTranslationUnit unit = nullptr;
    const auto error = clang_parseTranslationUnit2FullArgv(
        index, nullptr, arguments.data(), static_cast<int>(arguments.size()),
        nullptr, 0, CXTranslationUnit_DetailedPreprocessingRecord, &unit);
    if (error != CXError_Success || unit == nullptr)
    {
        std::cerr << "AST parse failed for "
                  << quality::detail::text(
                         clang_CompileCommand_getFilename(command))
                  << '\n';
        return false;
    }
    const bool valid = diagnostics_valid(unit);
    if (valid)
    {
        record_reachable(unit, command, state);
        inspect_translation_unit(unit, state);
    }
    clang_disposeTranslationUnit(unit);
    return valid;
}
} // namespace source_policy
