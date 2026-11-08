// Audits owned callable bodies and source contracts using compilation
// databases.
#include "source_contracts.h"

#include <clang-c/CXCompilationDatabase.h>
#include <clang-c/Index.h>

#include <exception>
#include <filesystem>
#include <iostream>
#include <set>
#include <stdexcept>
#include <string>
#include <vector>

namespace
{
// Copies a libclang string and releases its allocated storage.
std::string text(CXString value)
{
    const char* chars = clang_getCString(value);
    const std::string result = chars == nullptr ? "" : chars;
    clang_disposeString(value);
    return result;
}

struct State
{
    std::filesystem::path root;
    std::set<std::string> checked;
    quality::SourceContracts contracts;
    bool failed = false;
    unsigned body_limit = 60;
};

// Excludes dependencies and generated build files from authored-source checks.
bool owned(const std::string& file,
           const State& state)
{
    const auto relative =
        std::filesystem::path(file).lexically_relative(state.root);
    const auto first = relative.begin();
    return first != relative.end() &&
           (*first == "modules" || *first == "apps" || *first == "tools" ||
            *first == "benchmarks" || *first == "tests" ||
            *first == "include" || *first == "src" || *first == "examples");
}

// Counts physical body lines, including empty bodies and nested lambdas.
void check_body(CXCursor body,
                State& state)
{
    CXFile start_file = nullptr;
    CXFile end_file = nullptr;
    unsigned start_line = 0;
    unsigned end_line = 0;
    const CXSourceRange range = clang_getCursorExtent(body);
    clang_getSpellingLocation(clang_getRangeStart(range), &start_file,
                              &start_line, nullptr, nullptr);
    clang_getSpellingLocation(clang_getRangeEnd(range), &end_file, &end_line,
                              nullptr, nullptr);
    const std::string file = text(clang_getFileName(start_file));
    if (!owned(file, state))
    {
        return;
    }
    const std::string key = file + ":" + std::to_string(start_line);
    if (!state.checked.insert(key).second)
    {
        return;
    }
    if (start_file != end_file || end_line < start_line)
    {
        std::cerr << key << ": callable body has unsupported source range\n";
        state.failed = true;
        return;
    }
    const unsigned lines = end_line - start_line + 1;
    if (state.body_limit != 0 && lines > state.body_limit)
    {
        std::cerr << key << ": callable body has " << lines
                  << " physical lines; maximum " << state.body_limit << '\n';
        state.failed = true;
    }
}

// Visits authored callables without auditing third-party subtrees.
CXChildVisitResult visit(CXCursor cursor,
                         CXCursor,
                         CXClientData data)
{
    auto& state = *static_cast<State*>(data);
    const auto kind = clang_getCursorKind(cursor);
    const bool callable =
        kind == CXCursor_FunctionDecl || kind == CXCursor_CXXMethod ||
        kind == CXCursor_Constructor || kind == CXCursor_Destructor ||
        kind == CXCursor_ConversionFunction ||
        kind == CXCursor_FunctionTemplate || kind == CXCursor_LambdaExpr;
    CXFile source = nullptr;
    clang_getSpellingLocation(clang_getCursorLocation(cursor), &source, nullptr,
                              nullptr, nullptr);
    const bool authored =
        source != nullptr && owned(text(clang_getFileName(source)), state);
    if (authored && kind == CXCursor_MacroExpansion)
        state.contracts.inspect_macro(cursor);
    if (authored && callable && kind != CXCursor_LambdaExpr)
        state.contracts.inspect_function(cursor);
    if (callable)
    {
        clang_visitChildren(
            cursor,
            [](CXCursor child, CXCursor, CXClientData context)
            {
                const auto child_kind = clang_getCursorKind(child);
                if (child_kind == CXCursor_CompoundStmt ||
                    child_kind == CXCursor_CXXTryStmt)
                {
                    check_body(child, *static_cast<State*>(context));
                }
                return CXChildVisit_Continue;
            },
            data);
    }
    CXFile file = nullptr;
    clang_getSpellingLocation(clang_getCursorLocation(cursor), &file, nullptr,
                              nullptr, nullptr);
    if (file != nullptr && !owned(text(clang_getFileName(file)), state))
    {
        return CXChildVisit_Continue;
    }
    return CXChildVisit_Recurse;
}

// Records reachable files before inspecting authored declarations and bodies.
void audit_unit(CXTranslationUnit unit,
                CXCompileCommand command,
                State& state)
{
    state.contracts.parsed_file(
        text(clang_CompileCommand_getFilename(command)));
    clang_getInclusions(
        unit,
        [](CXFile file, CXSourceLocation*, unsigned, CXClientData data)
        {
            static_cast<State*>(data)->contracts.parsed_file(
                text(clang_getFileName(file)));
        },
        &state);
    clang_visitChildren(clang_getTranslationUnitCursor(unit), visit, &state);
}

// Parses compiler arguments faithfully and rejects errors before checking ASTs.
bool inspect(CXIndex index,
             CXCompileCommand command,
             State& state)
{
    std::vector<std::string> storage;
    const unsigned count = clang_CompileCommand_getNumArgs(command);
    storage.reserve(count);
    for (unsigned argument = 0; argument < count; ++argument)
    {
        const auto value = text(clang_CompileCommand_getArg(command, argument));
        if (value == "-c")
        {
            continue;
        }
        if (value == "-o")
        {
            ++argument;
            continue;
        }
        storage.push_back(value);
    }
    std::vector<const char*> arguments;
    storage.emplace_back("-fparse-all-comments");
    arguments.reserve(storage.size());
    for (const auto& argument : storage)
    {
        arguments.push_back(argument.c_str());
    }
    CXTranslationUnit unit = nullptr;
    const auto error = clang_parseTranslationUnit2FullArgv(
        index, nullptr, arguments.data(), static_cast<int>(arguments.size()),
        nullptr, 0, CXTranslationUnit_DetailedPreprocessingRecord, &unit);
    if (error != CXError_Success)
    {
        std::cerr << "AST parse failed: "
                  << text(clang_CompileCommand_getFilename(command)) << '\n';
        return false;
    }
    bool valid = true;
    for (unsigned i = 0; i < clang_getNumDiagnostics(unit); ++i)
    {
        CXDiagnostic diagnostic = clang_getDiagnostic(unit, i);
        if (clang_getDiagnosticSeverity(diagnostic) >= CXDiagnostic_Error)
        {
            std::cerr << text(clang_formatDiagnostic(
                             diagnostic,
                             clang_defaultDiagnosticDisplayOptions()))
                      << '\n';
            valid = false;
        }
        clang_disposeDiagnostic(diagnostic);
    }
    if (valid)
    {
        audit_unit(unit, command, state);
    }
    clang_disposeTranslationUnit(unit);
    return valid;
}
} // namespace

// Audits one database, retaining documentation records for subsequent
// consumers.
bool inspect_database(const char* directory,
                      State& state)
{
    CXCompilationDatabase_Error error;
    CXCompilationDatabase database =
        clang_CompilationDatabase_fromDirectory(directory, &error);
    if (error != CXCompilationDatabase_NoError)
    {
        std::cerr << "cannot load compilation database\n";
        return false;
    }
    CXIndex index = clang_createIndex(0, 0);
    CXCompileCommands commands =
        clang_CompilationDatabase_getAllCompileCommands(database);
    for (unsigned i = 0; i < clang_CompileCommands_getSize(commands); ++i)
    {
        CXCompileCommand command =
            clang_CompileCommands_getCommand(commands, i);
        if (owned(text(clang_CompileCommand_getFilename(command)), state) &&
            !inspect(index, command, state))
        {
            state.failed = true;
        }
    }
    clang_CompileCommands_dispose(commands);
    clang_CompilationDatabase_dispose(database);
    clang_disposeIndex(index);
    return !state.failed;
}

// Scans physical contracts even for sources not represented in a build.
void inspect_sources(State& state)
{
    for (const auto directory : {"modules", "apps", "benchmarks", "tests",
                                 "tools", "include", "src", "examples"})
    {
        const auto path = state.root / directory;
        if (!std::filesystem::exists(path))
            continue;
        for (const auto& entry :
             std::filesystem::recursive_directory_iterator(path))
        {
            if (entry.is_regular_file() && (entry.path().extension() == ".h" ||
                                            entry.path().extension() == ".cpp"))
                state.contracts.inspect_file(entry.path());
        }
    }
}

// Combines production and optional consumer ASTs for complete authored
// coverage.
int run(int argc,
        char** argv)
{
    if (argc < 3 || argc > 6 ||
        (argc >= 4 && std::string(argv[3]) != "--complete"))
    {
        std::cerr << "usage: source_policy <build-directory> "
                     "<repository-root> [--complete [consumer-build "
                     "[--contracts-only]]]\n";
        return 1;
    }
    State state{
        std::filesystem::absolute(argv[2]).lexically_normal(), {}, {}, false};
    if (argc == 6)
    {
        if (std::string(argv[5]) != "--contracts-only")
            throw std::invalid_argument("expected --contracts-only");
        state.body_limit = 0;
    }
    inspect_sources(state);
    state.failed = !inspect_database(argv[1], state);
    if (argc >= 5 && !inspect_database(argv[4], state))
        state.failed = true;
    const bool contracts_valid = state.contracts.finish(argc >= 4);
    return state.failed || !contracts_valid ? 1 : 0;
}

// Reports checker infrastructure failures as nonzero exits, not skipped checks.
int main(int argc,
         char** argv)
{
    try
    {
        return run(argc, argv);
    }
    catch (const std::exception& error)
    {
        std::cerr << "function-size checker failed: " << error.what() << '\n';
        return 1;
    }
}
