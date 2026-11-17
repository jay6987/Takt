// Owns scoped record bindings.
#pragma once

#include "takt/recordreplay/registry.h"

namespace takt::recordreplay
{
/// Move-only owner of one recording binding and its cleanup.
template <typename T> class ScopedRecord
{
  public:
    // Opens a recording binding owned by this scope.
    ScopedRecord(Pipe<T>& pipe,
                 std::string file_path)
        : pipe_(&pipe),
          binding_id_(
              detail::RecordReplayRegistry::instance().set_record(*pipe_,
                                                                  file_path)),
          active_(true)
    {
    }

    // Attempts cleanup without allowing failures to escape destruction.
    ~ScopedRecord() noexcept
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

    // A recording binding cannot have two scope owners.
    ScopedRecord(const ScopedRecord&) = delete;
    // Prevents duplicating recording cleanup responsibility.
    ScopedRecord& operator=(const ScopedRecord&) = delete;

    // Transfers binding ownership and deactivates the source scope.
    ScopedRecord(ScopedRecord&& rhs) noexcept
        : pipe_(std::exchange(rhs.pipe_,
                              nullptr)),
          binding_id_(std::exchange(rhs.binding_id_,
                                    0)),
          active_(rhs.active_)
    {
        rhs.active_ = false;
    }

    // Closes the previous binding before taking another scope's binding.
    ScopedRecord& operator=(ScopedRecord&& rhs)
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

        detail::RecordReplayRegistry::instance().clear_record(*pipe_,
                                                              binding_id_);
        pipe_ = nullptr;
        binding_id_ = 0;
        active_ = false;
    }

    // Reports whether this scope still owns an active recording binding.
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
