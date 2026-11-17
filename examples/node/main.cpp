// Demonstrates source, processing and sink nodes over configured frame pipes.
#include "double_node.h"

#include <iostream>
#include <memory>
#include <vector>

#include "takt/takt.h"

namespace
{
// Configures and initializes the standalone node's boundary pipes.
void initialize_pipes(std::shared_ptr<takt::Pipe<int>>& in,
                      std::shared_ptr<takt::Pipe<int>>& out)
{
    in = std::make_shared<takt::Pipe<int>>("node-in");
    in->set_producer(
        {"source", takt::concurrency(1), takt::frames_per_write(1)}, 0);
    in->set_consumer({"double", takt::concurrency(1), takt::frames_per_read(1),
                      takt::overlap_frames(0)});
    in->set_slot_capacity(16);
    out = std::make_shared<takt::Pipe<int>>("node-out");
    out->set_producer(
        {"double", takt::concurrency(1), takt::frames_per_write(1)}, 0);
    out->set_consumer({"sink", takt::concurrency(1), takt::frames_per_read(1),
                       takt::overlap_frames(0)});
    out->set_slot_capacity(16);
    in->initialize();
    out->initialize();
}
} // namespace

// Executes the doubling stage and verifies its public pipe outputs.
int main()
{
    std::shared_ptr<takt::Pipe<int>> in;
    std::shared_ptr<takt::Pipe<int>> out;
    initialize_pipes(in, out);
    takt_example::DoubleNode node(in, out);
    node.set_stage_name("double-stage");
    node.add_input_port_name("raw");
    node.add_output_port_name("result");

    node.start();
    for (int i = 1; i <= 4; ++i)
    {
        auto write = in->acquire_shot_end_write(1);
        write.value() = i;
        write.publish();
    }
    in->close();
    node.join();

    std::vector<int> result;
    while (true)
    {
        try
        {
            auto read = out->acquire_read();
            result.push_back(read.value());
        }
        catch (const takt::PipeClosedAndEmptySignal&)
        {
            break;
        }
    }

    std::cout << "node outputs:";
    for (int value : result)
    {
        std::cout << " " << value;
    }
    std::cout << std::endl;

    return result == std::vector<int>{2, 4, 6, 8} ? 0 : 1;
}
