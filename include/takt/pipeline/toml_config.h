// Provides optional TOML loading and static DOT export for pipeline graphs.
#pragma once

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <map>
#include <optional>
#include <string>

namespace takt
{
enum class CapacityMode : std::uint8_t
{
    Minimal,
    Fixed,
    MaxConcurrency,
};

struct PipeConfig
{
    std::string from;
    std::string to;
    CapacityMode mode = CapacityMode::Minimal;
    std::optional<std::size_t> slots;
};

struct NodeConfig
{
    std::string type;
    std::string subgraph;
    std::optional<std::size_t> workers;
};

struct GraphConfig
{
    std::map<std::string, NodeConfig> nodes;
    std::map<std::string, PipeConfig> pipes;
    std::map<std::string, std::string> inputs;
    std::map<std::string, std::string> outputs;
};

struct PipelineConfig
{
    std::size_t default_workers = 1;
    GraphConfig root;
    std::map<std::string, GraphConfig> subgraphs;
};

// Reads TOML graph files and resolves relative subgraph references.
[[nodiscard]] PipelineConfig
read_pipeline_toml(const std::filesystem::path &path);

// Renders a TOML-declared graph without creating runtime nodes or workers.
[[nodiscard]] std::string
export_pipeline_toml_dot(const std::filesystem::path &path,
                         bool expand_subgraphs = true,
                         bool show_interfaces = false);
} // namespace takt
