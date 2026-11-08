// Demonstrates configured pipe frames and scoped read/write token ownership.
#include <iostream>
#include <vector>

#include "takt/takt.h"

// Writes and reads example frames through explicitly initialized storage.
int main()
{
    takt::Pipe<int> numbers("numbers");
    numbers.set_producer(
        {"source", takt::concurrency(1), takt::frames_per_write(1)}, 0);
    numbers.set_consumer({"sink", takt::concurrency(1),
                          takt::frames_per_read(1), takt::overlap_frames(0)});
    numbers.set_slot_capacity(8);
    numbers.initialize();

    for (int i = 1; i <= 5; ++i)
    {
        auto w = numbers.acquire_shot_end_write(1);
        w.value() = i;
        w.publish();
    }
    numbers.close();

    std::vector<int> values;
    while (true)
    {
        try
        {
            auto r = numbers.acquire_read();
            values.push_back(r.value());
        }
        catch (const takt::PipeClosedAndEmptySignal&)
        {
            break;
        }
    }

    std::cout << "pipe values:";
    for (int v : values)
    {
        std::cout << " " << v;
    }
    std::cout << std::endl;

    return values.size() == 5 ? 0 : 1;
}
