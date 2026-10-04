include(FetchContent)
include(GoogleTest)

FetchContent_Declare(
    googletest
    URL https://github.com/google/googletest/archive/refs/tags/v1.14.0.zip
    DOWNLOAD_EXTRACT_TIMESTAMP TRUE
)

set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)

add_executable(takt_smoke_test
    pipe_builder_tests.cpp
    pipeline_builder_tests.cpp
    pipe_tests.cpp
    node_tests.cpp
)

target_link_libraries(takt_smoke_test
    PRIVATE
        takt::takt
        GTest::gtest_main
)

gtest_discover_tests(takt_smoke_test)
