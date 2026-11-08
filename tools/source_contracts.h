// Checks physical source layout and attached documentation for owned C++.
#pragma once

#include <clang-c/Index.h>

#include <filesystem>
#include <map>
#include <set>
#include <string>

namespace quality
{
struct FunctionContract
{
    std::string location;
    bool documented = false;
};

class SourceContracts
{
  public:
    // Records file-level rules once, including headers discovered by the AST.
    void inspect_file(const std::filesystem::path& path);
    // Records a written callable, merging documentation across redeclarations.
    void inspect_function(CXCursor cursor);
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
    bool failed_ = false;
};
} // namespace quality
