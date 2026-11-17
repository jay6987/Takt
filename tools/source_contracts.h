// Checks physical source layout and attached documentation for owned C++.
#pragma once

#include <clang-c/Index.h>

#include <filesystem>
#include <map>
#include <set>
#include <string>

namespace quality
{
/// Identifies a declaration and whether its source carries required prose.
struct FunctionContract
{
    std::string location;
    bool documented = false;
};

/// Records source contracts without depending on the command-line driver.
class SourceContracts
{
  public:
    // Records file-level rules once, including headers discovered by the AST.
    void inspect_file(const std::filesystem::path& path);
    // Tracks a file for reachability without applying physical source rules.
    void record_file(const std::filesystem::path& path);
    // Records a written callable, merging documentation across redeclarations.
    void inspect_function(CXCursor cursor);
    // Records a defined class or struct and its attached Doxygen description.
    void inspect_type(CXCursor cursor);
    // Checks written test macro invocations, excluding macro text in literals.
    void inspect_macro(CXCursor cursor);
    // Records files actually reached by a successfully parsed translation unit.
    void parsed_file(const std::filesystem::path& path);
    // Reports undocumented functions after all translation units are inspected.
    [[nodiscard]] bool finish(bool require_complete);

  private:
    // Rejects a file responsibility comment used as function documentation.
    [[nodiscard]] bool documented(CXCursor cursor) const;
    std::set<std::filesystem::path> files_;
    std::set<std::filesystem::path> parsed_;
    std::map<std::string, FunctionContract> functions_;
    std::map<std::string, FunctionContract> types_;
    bool failed_ = false;
};
} // namespace quality
