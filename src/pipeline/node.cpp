#include "takt/pipeline/node.h"

#include <exception>
#include <sstream>
#include <stdexcept>
#include <utility>

#include "takt/logging/logger.h"

namespace takt
{
namespace
{
std::string join_names(const std::vector<std::string>& names)
{
    std::ostringstream oss;
    for (size_t i = 0; i < names.size(); ++i)
    {
        if (i != 0)
        {
            oss << ",";
        }
        oss << names[i];
    }
    return oss.str();
}
} // namespace

NodeBase::NodeBase(std::string node_name) : name_(std::move(node_name)) {}

NodeBase::~NodeBase() = default;

const std::string& NodeBase::name() const noexcept
{
    return name_;
}

void NodeBase::set_stage_name(std::string stage_name)
{
    stage_name_ = std::move(stage_name);
}

void NodeBase::set_pipeline_name(std::string pipeline_name)
{
    pipeline_name_ = std::move(pipeline_name);
}

void NodeBase::add_input_port_name(std::string input_port_name)
{
    if (input_port_name.empty())
    {
        return;
    }
    input_port_names_.push_back(std::move(input_port_name));
}

void NodeBase::add_output_port_name(std::string output_port_name)
{
    if (output_port_name.empty())
    {
        return;
    }
    output_port_names_.push_back(std::move(output_port_name));
}

void NodeBase::clear_input_port_names() noexcept
{
    input_port_names_.clear();
}

void NodeBase::clear_output_port_names() noexcept
{
    output_port_names_.clear();
}

const std::string& NodeBase::stage_name() const noexcept
{
    return stage_name_;
}

const std::string& NodeBase::pipeline_name() const noexcept
{
    return pipeline_name_;
}

const std::vector<std::string>& NodeBase::input_port_names() const noexcept
{
    return input_port_names_;
}
const std::vector<std::string>& NodeBase::output_port_names() const noexcept
{
    return output_port_names_;
}

void NodeBase::request_stop() noexcept
{
    stop_requested_.store(true);
}

bool NodeBase::stop_requested() const noexcept
{
    return stop_requested_.load();
}

NodeExceptionContext NodeBase::build_exception_context(std::string message,
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

Node::Node(std::string node_name, size_t worker_count)
    : NodeBase(std::move(node_name)), worker_count_(worker_count)
{
    if (worker_count_ == 0)
    {
        throw std::invalid_argument("worker_count cannot be zero");
    }
}

Node::~Node()
{
    request_stop();
    join();
}

void Node::start()
{
    if (ingress_manager_.valid() || !workers_.empty())
    {
        return;
    }

    {
        std::lock_guard<std::mutex> lk(task_queue_mutex_);
        task_queue_.clear();
        task_queue_closed_ = false;
    }

    ingress_manager_ =
        std::async(std::launch::async, [this] { run_ingress_manager_wrapped(); });

    workers_.reserve(worker_count_);
    for (size_t i = 0; i < worker_count_; ++i)
    {
        workers_.emplace_back(
            std::async(std::launch::async, [this, i] { run_worker_wrapped(i); }));
    }
}

void Node::join()
{
    if (ingress_manager_.valid())
    {
        ingress_manager_.get();
    }

    for (auto& worker : workers_)
    {
        if (worker.valid())
        {
            worker.get();
        }
    }
    workers_.clear();
}

void Node::request_stop() noexcept
{
    NodeBase::request_stop();
    close_task_queue();
}

size_t Node::worker_count() const noexcept
{
    return worker_count_;
}

void Node::on_task_done(size_t worker_index, const Task& task)
{
    (void)worker_index;
    (void)task;
}

void Node::on_ingress_manager_done() {}

void Node::on_worker_done(size_t worker_index)
{
    (void)worker_index;
}

void Node::on_exception(const NodeExceptionContext& context)
{
    std::ostringstream oss;
    oss << "pipeline='" << context.pipeline_name << "' ";
    oss << "node='" << context.node_name << "' ";
    if (!context.stage_name.empty())
    {
        oss << "stage='" << context.stage_name << "' ";
    }
    if (context.has_worker_index)
    {
        oss << "worker='" << context.worker_index << "' ";
    }
    oss << "edge_in='" << join_names(context.input_port_names) << "' ";
    oss << "edge_out='" << join_names(context.output_port_names) << "' ";
    oss << "stop_reason='" << context.stop_reason << "' ";
    oss << "exception_summary='" << context.message << "'";
    error(oss.str());
}

void Node::run_ingress_manager_wrapped()
{
    try
    {
        uint64_t sequence_id = 0;
        while (!stop_requested())
        {
            auto task = build_task(sequence_id);
            if (!task)
            {
                break;
            }

            task->sequence_id = sequence_id;
            {
                std::lock_guard<std::mutex> lk(task_queue_mutex_);
                if (task_queue_closed_)
                {
                    break;
                }
                task_queue_.push_back(std::move(task));
            }
            task_queue_cv_.notify_one();
            ++sequence_id;
        }
    }
    catch (const std::exception& e)
    {
        on_exception(build_exception_context(e.what(), false, false, 0));
    }
    catch (...)
    {
        on_exception(build_exception_context("unknown exception", true, false, 0));
    }

    close_task_queue();
    on_ingress_manager_done();
}

bool Node::pop_task(std::unique_ptr<Task>& task)
{
    std::unique_lock<std::mutex> lk(task_queue_mutex_);
    task_queue_cv_.wait(
        lk, [this] { return task_queue_closed_ || !task_queue_.empty(); });
    if (task_queue_.empty())
    {
        return false;
    }

    task = std::move(task_queue_.front());
    task_queue_.pop_front();
    return true;
}

void Node::close_task_queue() noexcept
{
    {
        std::lock_guard<std::mutex> lk(task_queue_mutex_);
        if (task_queue_closed_)
        {
            return;
        }
        task_queue_closed_ = true;
    }
    task_queue_cv_.notify_all();
}

void Node::run_worker_wrapped(size_t worker_index)
{
    try
    {
        std::unique_ptr<Task> task;
        while (pop_task(task))
        {
            if (!task)
            {
                continue;
            }
            if (task->kind == TaskKind::Stop)
            {
                break;
            }

            process_task(worker_index, *task);
            on_task_done(worker_index, *task);
            task.reset();
        }
    }
    catch (const std::exception& e)
    {
        const bool has_worker_index = worker_count_ > 1;
        on_exception(
            build_exception_context(e.what(), false, has_worker_index, worker_index));
    }
    catch (...)
    {
        const bool has_worker_index = worker_count_ > 1;
        on_exception(build_exception_context("unknown exception", true,
                                             has_worker_index, worker_index));
    }

    on_worker_done(worker_index);
}
} // namespace takt
