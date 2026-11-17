// Runs the standalone Takt source readability audit.
#include "source_policy.h"

#include <exception>
#include <iostream>
#include <stdexcept>

namespace
{
// Preserves contracts-only fixtures while defaulting all invocations to strict.
int run(int argc,
        char** argv)
{
    if (argc < 3 || argc > 6)
    {
        std::cerr << "usage: source_policy <build-directory> "
                     "<repository-root> [--complete [consumer-build]] "
                     "[--contracts-only]\n";
        return 1;
    }
    source_policy::State state(argv[2]);
    bool complete = false;
    std::string consumer;
    int next = 3;
    if (next < argc && std::string(argv[next]) == "--complete")
    {
        complete = true;
        ++next;
    }
    if (complete && next < argc &&
        std::string(argv[next]) != "--contracts-only")
    {
        consumer = argv[next];
        ++next;
    }
    if (next < argc && std::string(argv[next]) == "--contracts-only")
    {
        state.contracts_only = true;
        ++next;
    }
    if (next != argc)
        throw std::invalid_argument("unexpected source_policy argument");

    source_policy::inspect_sources(state);
    if (!source_policy::inspect_database(argv[1], state))
        state.failed = true;
    if (!consumer.empty() && !source_policy::inspect_database(consumer, state))
        state.failed = true;
    const bool contracts_valid = state.contracts.finish(complete);
    return state.failed || !contracts_valid ? 1 : 0;
}
} // namespace

// Reports checker infrastructure failures as nonzero exits, never as skips.
int main(int argc,
         char** argv)
{
    try
    {
        return run(argc, argv);
    }
    catch (const std::exception& error)
    {
        std::cerr << "source readability checker failed: " << error.what()
                  << '\n';
        return 1;
    }
}
