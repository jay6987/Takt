// Declares state and operations for the standalone source readability audit.
#pragma once

#include "source_contracts.h"

#include <clang-c/CXCompilationDatabase.h>
#include <clang-c/Index.h>

#include <filesystem>
#include <set>
#include <string>

namespace source_policy
{
/// Holds normalized root paths and deduplicated AST body audit state.
struct State
{
    /// Initializes a strict audit rooted at one Takt checkout.
    explicit State(std::filesystem::path repository_root);

    std::filesystem::path root;
    std::set<std::string> checked_bodies;
    quality::SourceContracts contracts;
    bool failed = false;
    bool contracts_only = false;
    unsigned body_limit = 40;
};

/// Determines whether a source path belongs to an owned project directory.
bool is_owned(const std::filesystem::path& path,
              const State& state);

/// Checks the direct bodies belonging to one callable AST cursor.
void inspect_callable_bodies(CXCursor callable,
                             State& state);

/// Checks declarations and effective body lines in a parsed translation unit.
void inspect_translation_unit(CXTranslationUnit unit,
                              State& state);

/// Parses and audits one compilation command with its original flags.
bool inspect_command(CXIndex index,
                     CXCompileCommand command,
                     State& state);

/// Audits every owned command in one compilation database.
bool inspect_database(const std::filesystem::path& directory,
                      State& state);

/// Inspects all owned source files even when they are unreachable.
void inspect_sources(State& state);
} // namespace source_policy
