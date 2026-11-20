// Verifies the optional TOML API from an installed Takt package.
#include <takt/pipeline/toml_config.h>

#include <filesystem>

int main(int argc, char **argv)
{
    if (argc != 2)
        return 1;
    const auto config = takt::read_pipeline_toml(std::filesystem::path(argv[1]));
    return config.default_workers == 3 && config.root.nodes.size() == 2 ? 0 : 1;
}
