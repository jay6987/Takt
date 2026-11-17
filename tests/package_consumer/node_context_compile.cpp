// Checks the installed exception context header independently of node.h.
#include <takt/pipeline/node_exception_context.h>

#include <cstddef>
#include <string>
#include <type_traits>
#include <vector>

static_assert(std::is_aggregate_v<takt::NodeExceptionContext>);
static_assert(
    std::is_same_v<decltype(takt::NodeExceptionContext::pipeline_name),
                   std::string>);
static_assert(std::is_same_v<decltype(takt::NodeExceptionContext::node_name),
                             std::string>);
static_assert(std::is_same_v<decltype(takt::NodeExceptionContext::stage_name),
                             std::string>);
static_assert(
    std::is_same_v<decltype(takt::NodeExceptionContext::input_port_names),
                   std::vector<std::string>>);
static_assert(
    std::is_same_v<decltype(takt::NodeExceptionContext::output_port_names),
                   std::vector<std::string>>);
static_assert(std::is_same_v<decltype(takt::NodeExceptionContext::stop_reason),
                             std::string>);
static_assert(std::is_same_v<decltype(takt::NodeExceptionContext::message),
                             std::string>);
static_assert(
    std::is_same_v<decltype(takt::NodeExceptionContext::unknown_exception),
                   bool>);
static_assert(
    std::is_same_v<decltype(takt::NodeExceptionContext::has_worker_index),
                   bool>);
static_assert(std::is_same_v<decltype(takt::NodeExceptionContext::worker_index),
                             size_t>);
