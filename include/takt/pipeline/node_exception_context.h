// Describes the stage, ports, worker, and original cause of a node failure.
#pragma once

#include <cstddef>
#include <string>
#include <vector>

namespace takt
{
/// Carries the owning pipeline and execution location of a retained failure.
struct NodeExceptionContext
{
    std::string pipeline_name;
    std::string node_name;
    std::string stage_name;
    std::vector<std::string> input_port_names;
    std::vector<std::string> output_port_names;
    std::string stop_reason;
    std::string message;
    bool unknown_exception = false;
    bool has_worker_index = false;
    size_t worker_index = 0;
};
} // namespace takt
