#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

namespace takt
{
struct WorkerStatistics
{
    std::uint64_t tasks_started = 0;
    std::uint64_t tasks_succeeded = 0;
    std::uint64_t tasks_failed = 0;
    std::chrono::nanoseconds work_time{};
    std::chrono::nanoseconds task_wait_time{};
};

struct IngressStatistics
{
    std::chrono::nanoseconds worker_wait_time{};
    std::chrono::nanoseconds task_build_time{};
    std::chrono::nanoseconds drain_wait_time{};
    std::chrono::nanoseconds first_task_build_time{};
    std::chrono::nanoseconds in_round_task_build_time{};
};

struct NodeStatistics
{
    std::string path;
    IngressStatistics ingress;
    // Indexed by the fixed worker index, including workers with no tasks.
    std::vector<WorkerStatistics> workers;
};

struct PipeWaitStatistics
{
    std::uint64_t count = 0;
    std::chrono::nanoseconds time{};
};

struct PipeStatistics
{
    std::string path;
    std::size_t slot_capacity = 0;
    std::size_t peak_unreusable_slots = 0;
    PipeWaitStatistics read_wait;
    PipeWaitStatistics write_wait;
};

struct RuntimeStatistics
{
    std::vector<NodeStatistics> nodes;
    std::vector<PipeStatistics> pipes;
};
} // namespace takt
