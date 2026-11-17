// Provides type-erased record and replay registry operations.
#pragma once

#include "takt/recordreplay/pipe_codec.h"

#include <cstdint>
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

namespace detail
{
/// Synchronizes codecs and per-pipe recording and replay bindings.
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
                             const std::string& file_path);

    // Validates codec availability and installs an owning replay binding.
    template <typename T>
    std::uint64_t set_replay(Pipe<T>& pipe,
                             const std::string& file_path);

    // Removes recording only if the binding still belongs to this scope.
    template <typename T>
    void clear_record(Pipe<T>& pipe,
                      std::uint64_t binding_id);

    // Removes replay only if the binding still belongs to this scope.
    template <typename T>
    void clear_replay(Pipe<T>& pipe,
                      std::uint64_t binding_id);

  public:
    // Attempts bound recording during token cleanup without throwing.
    template <typename T>
    void try_record_slots(Pipe<T>& pipe,
                          const std::vector<size_t>& slots,
                          const std::vector<T>& frames) noexcept;

    // Restores bound frame slots, propagating codec or stream failures.
    template <typename T>
    void replay_slots(Pipe<T>& pipe,
                      const std::vector<size_t>& slots,
                      std::vector<T>& frames);

  private:
    /// Codec callbacks adapted to type-erased frame pointers.
    struct TypeErasedCodec
    {
        std::function<void(std::ostream&, const void*)> record_one;
        std::function<void(std::istream&, void*)> replay_one;
    };

    struct PipeChannel;

    // Wraps typed codec callbacks with checked construction of erased hooks.
    template <typename T>
    static TypeErasedCodec make_erased_codec(PipeCodec<T> codec);

    // Uses a registered codec or raw bytes for trivially copyable frames.
    template <typename T> TypeErasedCodec resolve_codec_for_type() const;

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

} // namespace recordreplay
} // namespace takt

#include "takt/recordreplay/registry_bindings.h"
#include "takt/recordreplay/registry_codec.h"
#include "takt/recordreplay/registry_slots.h"
