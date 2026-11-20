// Tests optional TOML parsing and static export for generic node types.
#include <takt/pipeline/toml_config.h>

#include <gtest/gtest.h>

#include <filesystem>
#include <string>

namespace
{
const std::filesystem::path fixtures = TAKT_TOML_FIXTURE_DIR;

TEST(PipelineTomlConfig, LoadsGenericNodesAndRelativeSubgraphs)
{
    const auto config =
        takt::read_pipeline_toml(fixtures / "root.toml");

    ASSERT_EQ(config.default_workers, 3U);
    ASSERT_EQ(config.root.nodes.size(), 2U);
    EXPECT_EQ(config.root.nodes.at("source").type, "custom.source");
    const auto subgraph = config.root.nodes.at("wrapper").subgraph;
    ASSERT_FALSE(subgraph.empty());
    ASSERT_EQ(config.subgraphs.size(), 1U);
    EXPECT_EQ(config.subgraphs.at(subgraph).inputs.at("in"), "scale.in");
    EXPECT_EQ(config.subgraphs.at(subgraph).outputs.at("out"), "scale.out");
    EXPECT_EQ(config.root.pipes.at("source-wrapper").mode,
              takt::CapacityMode::Fixed);
    EXPECT_EQ(config.root.pipes.at("source-wrapper").slots, 2U);
}

TEST(PipelineTomlConfig, ExportsWithoutKnowingNodeImplementations)
{
    const auto dot = takt::export_pipeline_toml_dot(fixtures / "root.toml");

    EXPECT_NE(dot.find("source-wrapper [fixed: 2]"), std::string::npos);
    EXPECT_NE(dot.find("source"), std::string::npos);
    EXPECT_NE(dot.find("wrapper"), std::string::npos);
    EXPECT_NE(dot.find("direct/shared Pipe: source-wrapper"), std::string::npos);
}
} // namespace
