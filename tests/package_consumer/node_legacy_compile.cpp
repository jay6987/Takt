// Checks the installed legacy node header and derived-node override contracts.
#include <takt/pipeline/node.h>

#include <cstdint>
#include <memory>
#include <type_traits>

namespace
{
/// Implements every extension hook through the legacy Node public API.
class LegacyNode final : public takt::Node
{
  public:
    using Node::Node;

    // Preserves the cancellation override signature.
    void request_stop() noexcept override {}

  private:
    // Preserves the task producer override signature.
    std::unique_ptr<Task> build_task(uint64_t) override
    {
        return nullptr;
    }
    // Preserves the worker processing override signature.
    void process_task(size_t,
                      Task&) override
    {
    }
    // Preserves the completed-task hook signature.
    void on_task_done(size_t,
                      const Task&) override
    {
    }
    // Preserves the barrier-completion hook signature.
    void on_barrier_complete(uint64_t) override {}
    // Preserves the ingress-completion hook signature.
    void on_ingress_manager_done() override {}
    // Preserves the worker-completion hook signature.
    void on_worker_done(size_t) override {}
    // Preserves the structured exception hook signature.
    void on_exception(const takt::NodeExceptionContext&) override {}
};

static_assert(std::is_base_of_v<takt::NodeBase,
                                takt::Node>);
static_assert(!std::is_abstract_v<LegacyNode>);
static_assert(std::is_same_v<decltype(takt::Node::Task::kind),
                             takt::Node::TaskKind>);
static_assert(std::is_same_v<decltype(takt::Node::Task::sequence_id),
                             uint64_t>);
static_assert(std::is_same_v<decltype(takt::Node::Task::barrier),
                             bool>);
static_assert(std::has_virtual_destructor_v<takt::Node::Task>);
static_assert(takt::Node::TaskKind::Normal != takt::Node::TaskKind::Stop);
} // namespace
