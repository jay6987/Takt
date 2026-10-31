#include <takt/takt.h>

int main()
{
    takt::Pipe<int> pipe("installed-package", 2, 0);
    {
        auto write = pipe.acquire_write(true);
        write.value() = 42;
        write.publish();
    }

    auto read = pipe.acquire_read();
    return read.value() == 42 ? 0 : 1;
}
