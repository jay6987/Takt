// Retains the first node failure and constructs its diagnostic context.
#include "takt/pipeline/node_base.h"

#include <utility>

namespace takt
{
void NodeBase::request_stop() noexcept
{
    stop_requested_.store(true);
}

bool NodeBase::stop_requested() const noexcept
{
    return stop_requested_.load();
}

std::exception_ptr NodeBase::failure() const
{
    std::lock_guard<std::mutex> lk(failure_mutex_);
    return failure_;
}

void NodeBase::remember_failure(std::exception_ptr failure)
{
    std::lock_guard<std::mutex> lk(failure_mutex_);
    if (!failure_)
    {
        failure_ = std::move(failure);
    }
}

NodeExceptionContext
NodeBase::build_exception_context(std::string message,
                                  bool unknown_exception,
                                  bool has_worker_index,
                                  size_t worker_index) const
{
    NodeExceptionContext context;
    context.pipeline_name = pipeline_name_;
    context.node_name = name_;
    context.stage_name = stage_name_;
    context.input_port_names = input_port_names_;
    context.output_port_names = output_port_names_;
    context.stop_reason = "exception";
    context.message = std::move(message);
    context.unknown_exception = unknown_exception;
    context.has_worker_index = has_worker_index;
    context.worker_index = worker_index;
    return context;
}
} // namespace takt
