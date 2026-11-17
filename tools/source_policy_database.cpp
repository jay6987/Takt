// Audits all project sources and all owned targets represented by a database.
#include "source_contracts_internal.h"
#include "source_policy.h"

#include <iostream>

namespace
{
// Recognizes maintained C++ implementation and header suffixes.
bool is_cpp_source(const std::filesystem::path& path)
{
    const auto extension = path.extension();
    return extension == ".cpp" || extension == ".cc" || extension == ".cxx" ||
           extension == ".c++" || extension == ".C" || extension == ".h" ||
           extension == ".hh" || extension == ".hpp" || extension == ".hxx" ||
           extension == ".ipp" || extension == ".tpp" || extension == ".inl" ||
           extension == ".mm";
}
} // namespace

namespace source_policy
{
bool inspect_database(const std::filesystem::path& directory,
                      State& state)
{
    CXCompilationDatabase_Error error;
    auto* database = clang_CompilationDatabase_fromDirectory(
        directory.string().c_str(), &error);
    if (error != CXCompilationDatabase_NoError || database == nullptr)
    {
        std::cerr << "cannot load compilation database: " << directory << '\n';
        return false;
    }
    auto* index = clang_createIndex(0, 0);
    auto* commands = clang_CompilationDatabase_getAllCompileCommands(database);
    bool valid = true;
    const auto count = clang_CompileCommands_getSize(commands);
    for (unsigned position = 0; position < count; ++position)
    {
        auto command = clang_CompileCommands_getCommand(commands, position);
        auto file = std::filesystem::path(
            quality::detail::text(clang_CompileCommand_getFilename(command)));
        if (!file.is_absolute())
            file = quality::detail::text(
                       clang_CompileCommand_getDirectory(command)) /
                   file;
        if (is_owned(file, state) && !inspect_command(index, command, state))
        {
            state.failed = true;
            valid = false;
        }
    }
    clang_CompileCommands_dispose(commands);
    clang_CompilationDatabase_dispose(database);
    clang_disposeIndex(index);
    return valid;
}

void inspect_sources(State& state)
{
    for (const auto* directory : {"modules", "apps", "benchmarks", "tests",
                                  "tools", "include", "src", "examples"})
    {
        const auto base = state.root / directory;
        if (!std::filesystem::exists(base))
            continue;
        for (const auto& entry :
             std::filesystem::recursive_directory_iterator(base))
        {
            if (!entry.is_regular_file() || !is_cpp_source(entry.path()))
                continue;
            if (state.contracts_only)
                state.contracts.record_file(entry.path());
            else
                state.contracts.inspect_file(entry.path());
        }
    }
}
} // namespace source_policy
