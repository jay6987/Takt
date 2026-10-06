#pragma once

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
    static RecordReplayRegistry& instance();

    template <typename T> void register_codec(PipeCodec<T> codec)
    {
        register_codec_impl(std::type_index(typeid(T)),
                            make_erased_codec<T>(std::move(codec)));
    }

    template <typename T> void set_record(Pipe<T>& pipe, const std::string& file_path)
    {
        static_cast<void>(resolve_codec_for_type<T>());
        set_record_impl(std::type_index(typeid(T)), &pipe, file_path);
    }

    template <typename T> void set_replay(Pipe<T>& pipe, const std::string& file_path)
    {
        static_cast<void>(resolve_codec_for_type<T>());
        set_replay_impl(std::type_index(typeid(T)), &pipe, file_path);
    }

    template <typename T> void clear_record(Pipe<T>& pipe)
    {
        clear_record_impl(std::type_index(typeid(T)), &pipe);
    }

    template <typename T> void clear_replay(Pipe<T>& pipe)
    {
        clear_replay_impl(std::type_index(typeid(T)), &pipe);
    }

    template <typename T>
    void try_record_slots(Pipe<T>& pipe, const std::vector<size_t>& slots,
                          const std::vector<T>& baskets) noexcept
    {
        try
        {
            const auto codec = resolve_codec_for_type<T>();
            try_record_slots_impl(std::type_index(typeid(T)), &pipe, slots,
                                  baskets.data(), sizeof(T), codec);
        }
        catch (...)
        {
            // Recording should not break production flow when triggered from token
            // cleanup.
        }
    }

    template <typename T>
    void replay_slots(Pipe<T>& pipe, const std::vector<size_t>& slots,
                      std::vector<T>& baskets)
    {
        const auto codec = resolve_codec_for_type<T>();
        replay_slots_impl(std::type_index(typeid(T)), &pipe, slots, baskets.data(),
                          sizeof(T), codec);
    }

  private:
    struct TypeErasedCodec
    {
        std::function<void(std::ostream&, const void*)> record_one;
        std::function<void(std::istream&, void*)> replay_one;
    };

    struct PipeChannel;

    template <typename T> static TypeErasedCodec make_erased_codec(PipeCodec<T> codec)
    {
        if (!codec.record_one || !codec.replay_one)
        {
            throw std::invalid_argument(
                "codec requires both record_one and replay_one");
        }

        TypeErasedCodec erased;
        erased.record_one =
            [fn = std::move(codec.record_one)](std::ostream& os, const void* value)
        { fn(os, *static_cast<const T*>(value)); };
        erased.replay_one =
            [fn = std::move(codec.replay_one)](std::istream& is, void* value)
        { fn(is, *static_cast<T*>(value)); };
        return erased;
    }

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

    void register_codec_impl(std::type_index type, TypeErasedCodec codec);
    void set_record_impl(std::type_index type, const void* pipe_id,
                         const std::string& file_path);
    void set_replay_impl(std::type_index type, const void* pipe_id,
                         const std::string& file_path);
    void clear_record_impl(std::type_index type, const void* pipe_id);
    void clear_replay_impl(std::type_index type, const void* pipe_id);
    std::optional<TypeErasedCodec> find_codec_impl(std::type_index type) const;
    void try_record_slots_impl(std::type_index type, const void* pipe_id,
                               const std::vector<size_t>& slots, const void* baskets,
                               size_t basket_size,
                               const TypeErasedCodec& codec) noexcept;
    void replay_slots_impl(std::type_index type, const void* pipe_id,
                           const std::vector<size_t>& slots, void* baskets,
                           size_t basket_size, const TypeErasedCodec& codec);
    std::shared_ptr<PipeChannel>
    get_or_create_pipe_channel_locked(const void* pipe_id, std::type_index type);

    mutable std::mutex mutex_;
    std::unordered_map<std::type_index, TypeErasedCodec> codecs_;
    std::unordered_map<const void*, std::shared_ptr<PipeChannel>> pipes_;
};
} // namespace detail

template <typename T> void register_codec(PipeCodec<T> codec)
{
    detail::RecordReplayRegistry::instance().register_codec<T>(std::move(codec));
}

template <typename T> void set_record(Pipe<T>& pipe, const std::string& file_path)
{
    detail::RecordReplayRegistry::instance().set_record(pipe, file_path);
}

template <typename T> void set_replay(Pipe<T>& pipe, const std::string& file_path)
{
    detail::RecordReplayRegistry::instance().set_replay(pipe, file_path);
}

template <typename T> void clear_record(Pipe<T>& pipe)
{
    detail::RecordReplayRegistry::instance().clear_record(pipe);
}

template <typename T> void clear_replay(Pipe<T>& pipe)
{
    detail::RecordReplayRegistry::instance().clear_replay(pipe);
}

template <typename T> class ScopedRecord
{
  public:
    ScopedRecord(Pipe<T>& pipe, std::string file_path)
        : pipe_(&pipe), active_(true)
    {
        set_record(*pipe_, file_path);
    }

    ~ScopedRecord()
    {
        reset();
    }

    ScopedRecord(const ScopedRecord&) = delete;
    ScopedRecord& operator=(const ScopedRecord&) = delete;

    ScopedRecord(ScopedRecord&& rhs) noexcept
        : pipe_(std::exchange(rhs.pipe_, nullptr)), active_(rhs.active_)
    {
        rhs.active_ = false;
    }

    ScopedRecord& operator=(ScopedRecord&& rhs) noexcept
    {
        if (this == &rhs)
        {
            return *this;
        }

        reset();
        pipe_ = std::exchange(rhs.pipe_, nullptr);
        active_ = rhs.active_;
        rhs.active_ = false;
        return *this;
    }

    void reset() noexcept
    {
        if (!pipe_ || !active_)
        {
            return;
        }

        try
        {
            clear_record(*pipe_);
        }
        catch (...)
        {
        }

        pipe_ = nullptr;
        active_ = false;
    }

    bool active() const noexcept
    {
        return active_;
    }

  private:
    Pipe<T>* pipe_ = nullptr;
    bool active_ = false;
};

template <typename T> class ScopedReplay
{
  public:
    ScopedReplay(Pipe<T>& pipe, std::string file_path)
        : pipe_(&pipe), active_(true)
    {
        set_replay(*pipe_, file_path);
    }

    ~ScopedReplay()
    {
        reset();
    }

    ScopedReplay(const ScopedReplay&) = delete;
    ScopedReplay& operator=(const ScopedReplay&) = delete;

    ScopedReplay(ScopedReplay&& rhs) noexcept
        : pipe_(std::exchange(rhs.pipe_, nullptr)), active_(rhs.active_)
    {
        rhs.active_ = false;
    }

    ScopedReplay& operator=(ScopedReplay&& rhs) noexcept
    {
        if (this == &rhs)
        {
            return *this;
        }

        reset();
        pipe_ = std::exchange(rhs.pipe_, nullptr);
        active_ = rhs.active_;
        rhs.active_ = false;
        return *this;
    }

    void reset() noexcept
    {
        if (!pipe_ || !active_)
        {
            return;
        }

        try
        {
            clear_replay(*pipe_);
        }
        catch (...)
        {
        }

        pipe_ = nullptr;
        active_ = false;
    }

    bool active() const noexcept
    {
        return active_;
    }

  private:
    Pipe<T>* pipe_ = nullptr;
    bool active_ = false;
};

template <typename T>
ScopedRecord<T> make_scoped_record(Pipe<T>& pipe, std::string file_path)
{
    return ScopedRecord<T>(pipe, std::move(file_path));
}

template <typename T>
ScopedReplay<T> make_scoped_replay(Pipe<T>& pipe, std::string file_path)
{
    return ScopedReplay<T>(pipe, std::move(file_path));
}

template <typename T> void attach_record(Pipe<T>& pipe, const std::string& file_path)
{
    set_record(pipe, file_path);
}

template <typename T> void attach_replay(Pipe<T>& pipe, const std::string& file_path)
{
    set_replay(pipe, file_path);
}
} // namespace recordreplay
} // namespace takt
