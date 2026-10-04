#include <iostream>
#include <vector>

#include "takt/takt.h"

int main()
{
    takt::Pipe<int> numbers("numbers", 8, 0);

    for (int i = 1; i <= 5; ++i)
    {
        auto w = numbers.acquire_write(true);
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
