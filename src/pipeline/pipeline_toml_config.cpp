// Parses versioned TOML pipeline graphs and resolves separate subgraph files.
#include "takt/pipeline/toml_config.h"

#include <toml++/toml.hpp>

#include <algorithm>
#include <fstream>
#include <limits>
#include <sstream>
#include <stdexcept>
#include <string_view>
#include <utility>
#include <vector>

namespace takt
{
namespace
{
[[noreturn]] void reject(const std::string &key, std::string_view message)
{
    throw std::invalid_argument(key + ": " + std::string(message));
}

void check_keys(const toml::table &table, const std::string &key,
                std::initializer_list<std::string_view> allowed)
{
    for (const auto &[name, value] : table)
        if (std::find(allowed.begin(), allowed.end(), name.str()) ==
            allowed.end())
            reject(key + "." + std::string(name.str()), "unknown field");
}

const toml::table &as_table(const toml::node &node, const std::string &key)
{
    if (const auto *table = node.as_table())
        return *table;
    reject(key, "expected a table");
}

std::string text(const toml::node *node, const std::string &key)
{
    if (node)
        if (const auto *value = node->as_string(); value && !value->get().empty())
            return value->get();
    reject(key, "expected a nonempty string");
}

std::optional<std::size_t> positive(const toml::table &table, const char *name,
                                    const std::string &key)
{
    const auto *node = table.get(name);
    if (!node)
        return {};
    const auto *value = node->as_integer();
    if (!value || value->get() < 1 ||
        static_cast<std::uint64_t>(value->get()) >
            std::numeric_limits<std::size_t>::max())
        reject(key + "." + name, "expected a positive integer");
    return static_cast<std::size_t>(value->get());
}

PipeConfig pipe_config(const toml::table &table, const std::string &key)
{
    check_keys(table, key, {"from", "to", "capacity"});
    PipeConfig result;
    result.from = text(table.get("from"), key + ".from");
    result.to = text(table.get("to"), key + ".to");
    if (const auto *node = table.get("capacity"))
    {
        const auto cap_key = key + ".capacity";
        const auto &capacity = as_table(*node, cap_key);
        check_keys(capacity, cap_key, {"mode", "slots"});
        const auto mode = text(capacity.get("mode"), cap_key + ".mode");
        if (mode == "fixed")
            result.mode = CapacityMode::Fixed;
        else if (mode == "max_concurrency")
            result.mode = CapacityMode::MaxConcurrency;
        else if (mode != "minimal")
            reject(cap_key, "expected fixed, minimal or max_concurrency");
        result.slots = positive(capacity, "slots", cap_key);
        if ((result.mode == CapacityMode::Fixed) != result.slots.has_value())
            reject(cap_key, "slots must be specified only for fixed capacity");
    }
    return result;
}

template <typename Parse>
auto named_entries(const toml::table &table, const char *name,
                   const std::string &key, Parse parse)
{
    using Value = decltype(parse(std::declval<const toml::node &>(), key));
    std::map<std::string, Value> result;
    if (const auto *node = table.get(name))
        for (const auto &[id, value] : as_table(*node, key + "." + name))
            result.emplace(id.str(), parse(value, key + "." + name + "." +
                                                  std::string(id.str())));
    return result;
}

struct Reader
{
    PipelineConfig result;
    std::map<std::filesystem::path, std::string> loaded;
    std::vector<std::filesystem::path> active;

    GraphConfig graph(const std::filesystem::path &path, bool root);
    std::string load_subgraph(const std::filesystem::path &path,
                              const std::string &key);
};

std::string Reader::load_subgraph(const std::filesystem::path &path,
                                  const std::string &key)
{
    std::error_code error;
    const auto canonical = std::filesystem::canonical(path, error);
    if (error)
        reject(key, "cannot resolve subgraph file '" + path.string() + "': " +
                        error.message());
    if (std::find(active.begin(), active.end(), canonical) != active.end())
        reject(key, "recursive subgraph file reference to '" +
                        canonical.string() + "'");
    if (const auto it = loaded.find(canonical); it != loaded.end())
        return it->second;
    auto definition = graph(canonical, false);
    const auto id = std::to_string(loaded.size());
    loaded.emplace(canonical, id);
    result.subgraphs.emplace(id, std::move(definition));
    return id;
}

GraphConfig Reader::graph(const std::filesystem::path &path, bool root)
{
    std::ifstream input(path);
    if (!input)
        reject(path.string(), "cannot open pipeline configuration");
    const auto key = path.string();
    const auto table = toml::parse(input, key);
    check_keys(table, key, {"version", "execution", "nodes", "pipes",
                            "inputs", "outputs", "subgraphs"});
    const auto *version = table.get("version");
    if (!version || !version->is_integer() || version->as_integer()->get() != 1)
        reject(key + ".version", "expected integer version 1");
    if (const auto *execution_node = table.get("execution"))
    {
        if (!root)
            reject(key + ".execution",
                   "execution settings belong in the root file");
        const auto &execution = as_table(*execution_node, key + ".execution");
        check_keys(execution, key + ".execution", {"default_workers"});
        const auto workers =
            positive(execution, "default_workers", key + ".execution");
        if (workers)
            result.default_workers = *workers;
    }

    active.push_back(path);
    const auto subgraphs = named_entries(
        table, "subgraphs", key,
        [&](const toml::node &value, const std::string &entry)
        {
            const auto &ref = as_table(value, entry);
            check_keys(ref, entry, {"file"});
            const auto file = text(ref.get("file"), entry + ".file");
            return load_subgraph(path.parent_path() /
                                     std::filesystem::u8path(file),
                                 entry);
        });
    active.pop_back();

    GraphConfig config;
    config.nodes = named_entries(
        table, "nodes", key,
        [&](const toml::node &value, const std::string &entry)
        {
            const auto &node = as_table(value, entry);
            check_keys(node, entry, {"type", "subgraph", "workers"});
            if (node.contains("type") == node.contains("subgraph"))
                reject(entry, "specify exactly one of type or subgraph");
            NodeConfig spec;
            if (const auto *alias = node.get("subgraph"))
            {
                const auto name = text(alias, entry + ".subgraph");
                const auto it = subgraphs.find(name);
                if (it == subgraphs.end())
                    reject(entry, "unknown subgraph alias '" + name + "'");
                spec.subgraph = it->second;
            }
            else
                spec.type = text(node.get("type"), entry + ".type");
            spec.workers = positive(node, "workers", entry);
            return spec;
        });
    config.pipes = named_entries(
        table, "pipes", key,
        [](const toml::node &value, const std::string &entry)
        { return pipe_config(as_table(value, entry), entry); });
    const auto port = [](const toml::node &value, const std::string &entry)
    { return text(&value, entry); };
    config.inputs = named_entries(table, "inputs", key, port);
    config.outputs = named_entries(table, "outputs", key, port);
    return config;
}
} // namespace

PipelineConfig read_pipeline_toml(const std::filesystem::path &path)
{
    Reader reader;
    try
    {
        reader.result.root =
            reader.graph(std::filesystem::canonical(path), true);
    }
    catch (const toml::parse_error &error)
    {
        std::ostringstream message;
        message << error;
        throw std::invalid_argument(message.str());
    }
    return std::move(reader.result);
}
} // namespace takt
