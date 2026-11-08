// Provides typed codecs and scoped recording/replay bindings for pipe frames.
#pragma once

#include <cstdint>
#include <fstream>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <typeindex>
#include <unordered_map>
#include <utility>
#include <vector>

namespace takt
{
template <typename T> class Pipe;

namespace recordreplay
{
template <typename T> class ScopedRecord;
template <typename T> class ScopedReplay;

template <typename T> struct PipeCodec
{
    std::function<void(std::ostream&, const T&)> record_one;
    std::function<void(std::istream&, T&)> replay_one;
};

namespace detail
{
class RecordReplayRegistry
{
  public:
    // Returns the process-wide registry for codecs and pipe bindings.
    static RecordReplayRegistry& instance();

    // Replaces the codec used for frames of this type.
    template <typename T> void register_codec(PipeCodec<T> codec)
    {
        register_codec_impl(std::type_index(typeid(T)),
                            make_erased_codec<T>(std::move(codec)));
    }

  private:
    template <typename T> friend class ::takt::recordreplay::ScopedRecord;
    template <typename T> friend class ::takt::recordreplay::ScopedReplay;

    // Validates codec availability and installs an owning recording binding.
    template <typename T>
    std::uint64_t set_record(Pipe<T>& pipe,
                             const std::string& file_path)
    {
        static_cast<void>(resolve_codec_for_type<T>());
        return set_record_impl(std::type_index(typeid(T)), &pipe, file_path);
    }

    // Validates codec availability and installs an owning replay binding.
    template <typename T>
    std::uint64_t set_replay(Pipe<T>& pipe,
                             const std::string& file_path)
    {
        static_cast<void>(resolve_codec_for_type<T>());
        return set_replay_impl(std::type_index(typeid(T)), &pipe, file_path);
    }

    // Removes recording only if the binding still belongs to this scope.
    template <typename T>
    void clear_record(Pipe<T>& pipe,
                      std::uint64_t binding_id)
    {
        clear_record_impl(std::type_index(typeid(T)), &pipe, binding_id);
    }

    // Removes replay only if the binding still belongs to this scope.
    template <typename T>
    void clear_replay(Pipe<T>& pipe,
                      std::uint64_t binding_id)
    {
        clear_replay_impl(std::type_index(typeid(T)), &pipe, binding_id);
    }

  public:
    // Attempts bound recording during token cleanup without throwing.
    template <typename T>
    void try_record_slots(Pipe<T>& pipe,
                          const std::vector<size_t>& slots,
                          const std::vector<T>& frames) noexcept
    {
        try
        {
            try_record_slots_impl(std::type_index(typeid(T)), &pipe, slots,
                                  frames.data(), sizeof(T), [this]
                                  { return resolve_codec_for_type<T>(); });
        }
        catch (...)
        {
            // Recording should not break production flow when triggered from
            // token cleanup.
        }
    }

    // Restores bound frame slots, propagating codec or stream failures.
    template <typename T>
    void replay_slots(Pipe<T>& pipe,
                      const std::vector<size_t>& slots,
                      std::vector<T>& frames)
    {
        replay_slots_impl(std::type_index(typeid(T)), &pipe, slots,
                          frames.data(), sizeof(T),
                          [this] { return resolve_codec_for_type<T>(); });
    }

  private:
    struct TypeErasedCodec
    {
        std::function<void(std::ostream&, const void*)> record_one;
        std::function<void(std::istream&, void*)> replay_one;
    };

    struct PipeChannel;

    // Wraps typed codec callbacks with checked construction of erased hooks.
    template <typename T>
    static TypeErasedCodec make_erased_codec(PipeCodec<T> codec)
    {
        if (!codec.record_one || !codec.replay_one)
        {
            throw std::invalid_argument(
                "codec requires both record_one and replay_one");
        }

        TypeErasedCodec erased;
        erased.record_one = [fn = std::move(codec.record_one)](
                                std::ostream& os, const void* value)
        { fn(os, *static_cast<const T*>(value)); };
        erased.replay_one =
            [fn = std::move(codec.replay_one)](std::istream& is, void* value)
        { fn(is, *static_cast<T*>(value)); };
        return erased;
    }

    // Uses a registered codec or raw bytes for trivially copyable frames.
    template <typename T> TypeErasedCodec resolve_codec_for_type() const
    {
        const auto registered = find_codec_impl(std::type_index(typeid(T)));
        if (registered.has_value())
        {
            return *registered;
        }

        if constexpr (std::is_trivially_copyable_v<T>)
        {
            TypeErasedCodec codec;
            codec.record_one = [](std::ostream& os, const void* value)
            { os.write(reinterpret_cast<const char*>(value), sizeof(T)); };
            codec.replay_one = [](std::istream& is, void* value)
            { is.read(reinterpret_cast<char*>(value), sizeof(T)); };
            return codec;
        }

        throw std::runtime_error(
            "no codec registered for non-trivially-copyable type; call "
            "takt::recordreplay::register_codec<T>() first");
    }

    // Stores type-erased callbacks under the registry lock.
    void register_codec_impl(std::type_index type,
                             TypeErasedCodec codec);
    // Opens and installs a recording stream with a fresh ownership identifier.
    std::uint64_t set_record_impl(std::type_index type,
                                  const void* pipe_id,
                                  const std::string& file_path);
    // Opens and installs a replay stream with a fresh ownership identifier.
    std::uint64_t set_replay_impl(std::type_index type,
                                  const void* pipe_id,
                                  const std::string& file_path);
    // Closes the matching owned recording stream.
    void clear_record_impl(std::type_index type,
                           const void* pipe_id,
                           std::uint64_t binding_id);
    // Closes the matching owned replay stream.
    void clear_replay_impl(std::type_index type,
                           const void* pipe_id,
                           std::uint64_t binding_id);
    // Looks up registered callbacks without exposing the registry lock.
    std::optional<TypeErasedCodec> find_codec_impl(std::type_index type) const;
    // Serializes committed slots if recording is bound to this pipe.
    void try_record_slots_impl(
        std::type_index type,
        const void* pipe_id,
        const std::vector<size_t>& slots,
        const void* frames,
        size_t frame_size,
        const std::function<TypeErasedCodec()>& resolve_codec) noexcept;
    // Restores requested slots if replay is bound to this pipe.
    void
    replay_slots_impl(std::type_index type,
                      const void* pipe_id,
                      const std::vector<size_t>& slots,
                      void* frames,
                      size_t frame_size,
                      const std::function<TypeErasedCodec()>& resolve_codec);
    // Finds or creates a type-consistent pipe channel under the registry lock.
    std::shared_ptr<PipeChannel>
    get_or_create_pipe_channel_locked(const void* pipe_id,
                                      std::type_index type);

    mutable std::mutex mutex_;
    std::unordered_map<std::type_index, TypeErasedCodec> codecs_;
    std::unordered_map<const void*, std::shared_ptr<PipeChannel>> pipes_;
};
} // namespace detail

// Registers custom serialization for a frame type before binding file scopes.
template <typename T> void register_codec(PipeCodec<T> codec)
{
    detail::RecordReplayRegistry::instance().register_codec<T>(
        std::move(codec));
}

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

// Returns a move-only recording scope for the supplied pipe and file.
template <typename T>
ScopedRecord<T> make_scoped_record(Pipe<T>& pipe,
                                   std::string file_path)
{
    return ScopedRecord<T>(pipe, std::move(file_path));
}

// Returns a move-only replay scope for the supplied pipe and file.
template <typename T>
ScopedReplay<T> make_scoped_replay(Pipe<T>& pipe,
                                   std::string file_path)
{
    return ScopedReplay<T>(pipe, std::move(file_path));
}
} // namespace recordreplay
} // namespace takt
