#pragma once

#include <chrono>

namespace takt
{
class StatisticsTimer
{
  public:
    explicit StatisticsTimer(std::chrono::nanoseconds* total) noexcept
        : total_(total), start_(total ? Clock::now() : Clock::time_point{})
    {
    }

    ~StatisticsTimer()
    {
        if (total_)
            *total_ += std::chrono::duration_cast<std::chrono::nanoseconds>(
                Clock::now() - start_);
    }

  private:
    using Clock = std::chrono::steady_clock;
    std::chrono::nanoseconds* total_;
    Clock::time_point start_;
};
} // namespace takt
