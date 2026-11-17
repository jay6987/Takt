// Owns scoped replay bindings.
#pragma once

#include "takt/recordreplay/registry.h"

namespace takt::recordreplay
{
/// Move-only owner of one replay binding and its cleanup.
template <typename T> class ScopedReplay
{
  public:
    // Opens a replay binding owned by this scope.
    ScopedReplay(Pipe<T>& pipe,
                 std::string file_path)
        : pipe_(&pipe),
          binding_id_(
              detail::RecordReplayRegistry::instance().set_replay(*pipe_,
                                                                  file_path)),
          active_(true)
    {
    }

    // Attempts cleanup without allowing failures to escape destruction.
    ~ScopedReplay() noexcept
    {
        try
        {
            reset();
        }
        catch (...)
        {
            // Unlike explicit reset(), destruction cannot report failures;
            // cleanup is best-effort, and active_ remains unchanged until this
            // object's lifetime ends.
        }
    }

    // A replay binding cannot have two scope owners.
    ScopedReplay(const ScopedReplay&) = delete;
    // Prevents duplicating replay cleanup responsibility.
    ScopedReplay& operator=(const ScopedReplay&) = delete;

    // Transfers binding ownership and deactivates the source scope.
    ScopedReplay(ScopedReplay&& rhs) noexcept
        : pipe_(std::exchange(rhs.pipe_,
                              nullptr)),
          binding_id_(std::exchange(rhs.binding_id_,
                                    0)),
          active_(rhs.active_)
    {
        rhs.active_ = false;
    }

    // Closes the previous binding before taking another scope's binding.
    ScopedReplay& operator=(ScopedReplay&& rhs)
    {
        if (this == &rhs)
        {
            return *this;
        }

        reset();
        pipe_ = std::exchange(rhs.pipe_, nullptr);
        binding_id_ = std::exchange(rhs.binding_id_, 0);
        active_ = rhs.active_;
        rhs.active_ = false;
        return *this;
    }

    // Explicitly closes this binding, propagating cleanup failures.
    void reset()
    {
        if (!pipe_ || !active_)
        {
            return;
        }

        detail::RecordReplayRegistry::instance().clear_replay(*pipe_,
                                                              binding_id_);
        pipe_ = nullptr;
        binding_id_ = 0;
        active_ = false;
    }

    // Reports whether this scope still owns an active replay binding.
    bool active() const noexcept
    {
        return active_;
    }

  private:
    Pipe<T>* pipe_ = nullptr;
    std::uint64_t binding_id_ = 0;
    bool active_ = false;
};

} // namespace takt::recordreplay
