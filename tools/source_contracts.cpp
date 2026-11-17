// Enforces physical source layout, file documentation and AST reachability.
#include "source_contracts.h"

#include "source_contracts_internal.h"

#include <fstream>
#include <iostream>
#include <stdexcept>

namespace quality
{
namespace
{
// Reports missing descriptions in one contract collection.
bool report_undocumented(const std::map<std::string,
                                        FunctionContract>& contracts,
                         const std::string& issue)
{
    bool valid = true;
    for (const auto& [key, contract] : contracts)
    {
        (void)key;
        if (!contract.documented)
        {
            std::cerr << contract.location << ": " << issue << '\n';
            valid = false;
        }
    }
    return valid;
}

// Checks the leading responsibility comment independently of line layout.
void inspect_file_header(const std::filesystem::path& path,
                         const std::string& source,
                         bool& failed)
{
    const auto header = detail::header_end(source);
    if (header == 0 || !detail::descriptive(source.substr(0, header)))
    {
        std::cerr << path << ": missing file responsibility comment\n";
        failed = true;
    }
}

// Counts physical lines and enforces the width and source-size caps.
void inspect_file_layout(const std::filesystem::path& path,
                         const std::string& source,
                         bool& failed)
{
    std::size_t column = 0;
    unsigned line = 1;
    unsigned physical_lines = 0;
    for (const char value : source)
    {
        if (value == '\n')
        {
            ++physical_lines;
            column = 0;
            ++line;
            continue;
        }
        if (value == '\r')
            continue;
        column += value == '\t' ? 4 - column % 4 : 1;
        if (column > 80)
        {
            std::cerr << path << ':' << line << ": line exceeds 80 columns\n";
            failed = true;
            column = 0;
        }
    }
    if (!source.empty() && source.back() != '\n')
        ++physical_lines;
    if (physical_lines > 150)
    {
        std::cerr << path << ": " << physical_lines
                  << " physical lines exceeds 150\n";
        failed = true;
    }
}
} // namespace

void SourceContracts::inspect_file(const std::filesystem::path& path)
{
    const auto normalized = std::filesystem::absolute(path).lexically_normal();
    if (!files_.insert(normalized).second)
        return;
    const auto source = detail::read_source(normalized);
    inspect_file_header(normalized, source, failed_);
    inspect_file_layout(normalized, source, failed_);
}

void SourceContracts::record_file(const std::filesystem::path& path)
{
    files_.insert(std::filesystem::absolute(path).lexically_normal());
}

void SourceContracts::parsed_file(const std::filesystem::path& path)
{
    parsed_.insert(std::filesystem::absolute(path).lexically_normal());
}

bool SourceContracts::finish(bool require_complete)
{
    if (require_complete)
    {
        for (const auto& file : files_)
        {
            if (!parsed_.contains(file))
            {
                std::cerr << file << ": source not reached by the build AST\n";
                failed_ = true;
            }
        }
    }
    failed_ |= !report_undocumented(functions_, "missing function comment");
    failed_ |= !report_undocumented(
        types_, "class/struct definition needs Doxygen documentation");
    return !failed_;
}
} // namespace quality
