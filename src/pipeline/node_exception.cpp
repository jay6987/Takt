// Reports structured node exceptions without replacing the original failure.
#include "takt/pipeline/node.h"

#include "takt/logging/logger.h"

#include <sstream>

namespace takt
{
namespace
{
// Joins port labels for structured exception logging.
std::string join_names(const std::vector<std::string>& names)
{
    std::ostringstream oss;
    for (size_t i = 0; i < names.size(); ++i)
    {
        if (i != 0)
            oss << ",";
        oss << names[i];
    }
    return oss.str();
}
} // namespace

void Node::on_exception(const NodeExceptionContext& context)
{
    std::ostringstream oss;
    oss << "pipeline='" << context.pipeline_name << "' ";
    oss << "node='" << context.node_name << "' ";
    if (!context.stage_name.empty())
        oss << "stage='" << context.stage_name << "' ";
    if (context.has_worker_index)
        oss << "worker='" << context.worker_index << "' ";
    oss << "edge_in='" << join_names(context.input_port_names) << "' ";
    oss << "edge_out='" << join_names(context.output_port_names) << "' ";
    oss << "stop_reason='" << context.stop_reason << "' ";
    oss << "exception_summary='" << context.message << "'";
    error(oss.str());
}

void Node::handle_exception(bool has_worker_index,
                            size_t worker_index) noexcept
{
    const auto cause = std::current_exception();
    remember_failure(cause);
    request_stop();
    try
    {
        on_exception(describe_exception(cause, has_worker_index, worker_index));
    }
    catch (...)
    {
        error("node exception observer threw; preserving original failure");
    }
}

NodeExceptionContext Node::describe_exception(std::exception_ptr cause,
                                              bool has_worker_index,
                                              size_t worker_index) const
{
    try
    {
        std::rethrow_exception(cause);
    }
    catch (const std::exception& e)
    {
        return build_exception_context(e.what(), false, has_worker_index,
                                       worker_index);
    }
    catch (...)
    {
        return build_exception_context("unknown exception", true,
                                       has_worker_index, worker_index);
    }
}
} // namespace takt
