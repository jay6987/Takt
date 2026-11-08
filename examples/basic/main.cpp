// Demonstrates the public umbrella header and default console logger.
#include "takt/takt.h"

// Emits a greeting through the default logger.
int main()
{
    takt::info("Takt basic example started");
    takt::warn(
        "Inject your own logger by implementing takt::ILogger and calling "
        "takt::set_logger().");
    return 0;
}
