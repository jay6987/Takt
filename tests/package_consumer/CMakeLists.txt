cmake_minimum_required(VERSION 3.20)

project(TaktInstalledPackageSmoke LANGUAGES CXX)

find_package(Takt 0.2 CONFIG REQUIRED)

add_executable(takt_package_consumer
    main.cpp
    node_base_compile.cpp
    node_context_compile.cpp
    node_legacy_compile.cpp
)
target_link_libraries(takt_package_consumer PRIVATE takt::takt)
set_target_properties(takt_package_consumer PROPERTIES
    CXX_STANDARD 17
    CXX_STANDARD_REQUIRED YES
    CXX_EXTENSIONS NO
)
enable_testing()
add_test(NAME TaktInstalledConsumerRuns COMMAND takt_package_consumer)
