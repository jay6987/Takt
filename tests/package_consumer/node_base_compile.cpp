// Checks the installed NodeBase header without including the legacy Node API.
#include <takt/pipeline/node_base.h>

#include <memory>
#include <type_traits>
#include <utility>
#include <vector>

namespace
{
/// Implements the direct NodeBase lifecycle and diagnostics interface.
class DirectNode final : public takt::NodeBase
{
  public:
    using NodeBase::NodeBase;

    // Implements the public lifecycle hooks.
    void start() override {}
    // Implements the public join hook.
    void join() override {}
    // Preserves the noexcept cancellation override.
    void request_stop() noexcept override {}
    // Preserves the nested-runtime override signature.
    std::vector<std::shared_ptr<takt::PipelineRuntime>>
    child_runtimes() const override
    {
        return {};
    }

    // Exercises the protected structured-context API.
    takt::NodeExceptionContext context() const
    {
        return build_exception_context("failure", false, true, 1);
    }
};

static_assert(!std::is_abstract_v<DirectNode>);
static_assert(
    std::is_same_v<decltype(std::declval<const DirectNode&>().context()),
                   takt::NodeExceptionContext>);
} // namespace
