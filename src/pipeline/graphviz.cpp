// Exports validated pipeline graphs in DOT format.
#include "graphviz_detail.h"

#include <sstream>

namespace takt
{
using namespace dot_detail;
std::string to_dot(const PipelineGraph& graph,
                   DotOptions options)
{
    if (options.view != DotView::Connections &&
        options.view != DotView::Interfaces)
        throw std::invalid_argument("DOT options have invalid view");
    validate_graph(graph);
    std::ostringstream output;
    output << "digraph pipeline {\n  graph [label=" << quoted(graph.name)
           << ",labelloc=t];\n  rankdir="
           << (options.left_to_right ? "LR" : "TB") << ";\n";
    write_graph(output, graph, "g_", options);
    output << "}\n";
    return output.str();
}
} // namespace takt
