#include "takt/takt.h"

int main()
{
    takt::info("Takt basic example started");
    takt::warn("Inject your own logger by implementing takt::ILogger and calling "
               "takt::set_logger().");
    return 0;
}
