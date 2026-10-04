cmake_minimum_required(VERSION 3.20)

project(Takt VERSION 0.1.0 LANGUAGES CXX)

option(TAKT_BUILD_EXAMPLES "Build Takt examples" ON)
option(TAKT_BUILD_TESTS "Build Takt tests" ON)
option(TAKT_WARNINGS_AS_ERRORS "Treat warnings as errors" ON)

add_library(takt STATIC
    src/recordreplay/recordreplay.cpp
    src/logging/global_logger.cpp
    src/pipeline/node.cpp
    src/pipeline/subgraph_node.cpp
    src/pipeline/builder.cpp
    src/pipeline/pipeline_builder.cpp
    src/pipeline/pipeline_runtime.cpp
    src/pipeline/pipe_core.cpp
)

add_library(takt::takt ALIAS takt)

target_compile_features(takt PUBLIC cxx_std_17)

target_include_directories(takt
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/include
)

if(MSVC)
    target_compile_options(takt PRIVATE /W4)
    if(TAKT_WARNINGS_AS_ERRORS)
        target_compile_options(takt PRIVATE /WX)
    endif()
else()
    target_compile_options(takt PRIVATE -Wall -Wextra -Wpedantic)
    if(TAKT_WARNINGS_AS_ERRORS)
        target_compile_options(takt PRIVATE -Werror)
    endif()
endif()

if(TAKT_BUILD_EXAMPLES)
    add_subdirectory(examples)
endif()

if(TAKT_BUILD_TESTS)
    enable_testing()
    add_subdirectory(tests)
endif()
