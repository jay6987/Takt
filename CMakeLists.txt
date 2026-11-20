cmake_minimum_required(VERSION 3.20)

project(Takt VERSION 0.2.0 LANGUAGES CXX)

include(GNUInstallDirs)
include(CMakePackageConfigHelpers)

option(TAKT_BUILD_EXAMPLES "Build Takt examples" OFF)
option(TAKT_BUILD_TESTS "Build Takt tests" OFF)
option(TAKT_BUILD_TOML_CONFIG "Build the optional TOML pipeline configuration API" OFF)
option(TAKT_WARNINGS_AS_ERRORS "Treat warnings as errors" ON)
option(TAKT_COVERAGE "Instrument with LLVM source coverage" OFF)
option(TAKT_BUILD_QUALITY_TOOLS "Build the shared AST source checker" OFF)
set(TAKT_SANITIZER "none" CACHE STRING "none, address-undefined, or thread")
set_property(CACHE TAKT_SANITIZER PROPERTY STRINGS none address-undefined thread)

if(NOT TAKT_SANITIZER MATCHES "^(none|address-undefined|thread)$")
    message(FATAL_ERROR "Unknown TAKT_SANITIZER: ${TAKT_SANITIZER}")
endif()
if(TAKT_COVERAGE AND NOT TAKT_SANITIZER STREQUAL "none")
    message(FATAL_ERROR "Coverage and sanitizer builds must be separate")
endif()
if(TAKT_COVERAGE OR NOT TAKT_SANITIZER STREQUAL "none")
    if(NOT CMAKE_CXX_COMPILER_ID MATCHES "Clang" OR MSVC)
        message(FATAL_ERROR "Instrumentation requires Clang with the GNU driver")
    endif()
    if(TAKT_COVERAGE)
        add_compile_options(-O1 -fprofile-instr-generate -fcoverage-mapping)
        add_link_options(-fprofile-instr-generate)
    elseif(TAKT_SANITIZER STREQUAL "address-undefined")
        add_compile_options(-fsanitize=address,undefined -fno-omit-frame-pointer)
        add_link_options(-fsanitize=address,undefined)
    else()
        add_compile_options(-fsanitize=thread -fno-omit-frame-pointer)
        add_link_options(-fsanitize=thread)
    endif()
endif()

function(takt_warnings target)
    set_target_properties(${target} PROPERTIES CXX_EXTENSIONS OFF)
    if(MSVC)
        target_compile_options(${target} PRIVATE /W4 /permissive-)
        if(TAKT_WARNINGS_AS_ERRORS)
            target_compile_options(${target} PRIVATE /WX)
        endif()
    else()
        target_compile_options(${target} PRIVATE -Wall -Wextra -Wpedantic)
        if(TAKT_WARNINGS_AS_ERRORS)
            target_compile_options(${target} PRIVATE -Werror)
        endif()
    endif()
endfunction()

if(TAKT_BUILD_QUALITY_TOOLS)
    include(cmake/QualityTools.cmake)
    takt_add_source_checker(takt_source_policy)
    takt_warnings(takt_source_policy)
endif()

add_library(takt STATIC
    src/recordreplay/recordreplay.cpp
    src/recordreplay/record_bindings.cpp
    src/recordreplay/record_cleanup.cpp
    src/recordreplay/replay_slots.cpp
    src/logging/global_logger.cpp
    src/logging/default_logger.cpp
    src/logging/logger_format.cpp
    src/pipeline/node.cpp
    src/pipeline/node_base.cpp
    src/pipeline/node_dispatch.cpp
    src/pipeline/node_exception.cpp
    src/pipeline/node_failure.cpp
    src/pipeline/node_ingress.cpp
    src/pipeline/node_worker.cpp
    src/pipeline/subgraph_node.cpp
    src/pipeline/subgraph_bridge.cpp
    src/pipeline/subgraph_mappings.cpp
    src/pipeline/pipeline_builder.cpp
    src/pipeline/builder_lookup.cpp
    src/pipeline/builder_bridges.cpp
    src/pipeline/builder_validation.cpp
    src/pipeline/builder_ports.cpp
    src/pipeline/builder_connections.cpp
    src/pipeline/builder_subgraph.cpp
    src/pipeline/builder_queries.cpp
    src/pipeline/builder_runtime.cpp
    src/pipeline/pipeline_runtime.cpp
    src/pipeline/runtime_initialization.cpp
    src/pipeline/runtime_statistics.cpp
    src/pipeline/pipe_base.cpp
    src/pipeline/pipe_core.cpp
    src/pipeline/pipe_core_write.cpp
    src/pipeline/pipe_core_read.cpp
    src/pipeline/pipe_core_slots.cpp
    src/pipeline/pipe_core_release.cpp
    src/pipeline/graphviz.cpp
    src/pipeline/graphviz_validation.cpp
    src/pipeline/graphviz_helpers.cpp
    src/pipeline/graphviz_endpoint.cpp
    src/pipeline/graphviz_ports.cpp
    src/pipeline/graphviz_mapping.cpp
    src/pipeline/graphviz_render.cpp
)

add_library(takt::takt ALIAS takt)

target_compile_features(takt PUBLIC cxx_std_17)

target_include_directories(takt
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
)

takt_warnings(takt)

set(TAKT_INSTALL_CMAKEDIR "${CMAKE_INSTALL_LIBDIR}/cmake/Takt")

install(TARGETS takt
    EXPORT TaktTargets
    ARCHIVE DESTINATION "${CMAKE_INSTALL_LIBDIR}"
    LIBRARY DESTINATION "${CMAKE_INSTALL_LIBDIR}"
    RUNTIME DESTINATION "${CMAKE_INSTALL_BINDIR}"
    INCLUDES DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}"
)
install(DIRECTORY include/ DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}")
install(EXPORT TaktTargets
    FILE TaktTargets.cmake
    NAMESPACE takt::
    DESTINATION "${TAKT_INSTALL_CMAKEDIR}"
)

configure_package_config_file(
    "${CMAKE_CURRENT_SOURCE_DIR}/cmake/TaktConfig.cmake.in"
    "${CMAKE_CURRENT_BINARY_DIR}/TaktConfig.cmake"
    INSTALL_DESTINATION "${TAKT_INSTALL_CMAKEDIR}"
)
write_basic_package_version_file(
    "${CMAKE_CURRENT_BINARY_DIR}/TaktConfigVersion.cmake"
    VERSION "${PROJECT_VERSION}"
    COMPATIBILITY SameMinorVersion
)
install(FILES
    "${CMAKE_CURRENT_BINARY_DIR}/TaktConfig.cmake"
    "${CMAKE_CURRENT_BINARY_DIR}/TaktConfigVersion.cmake"
    DESTINATION "${TAKT_INSTALL_CMAKEDIR}"
)

if(TAKT_BUILD_TOML_CONFIG)
    if(NOT TARGET tomlplusplus::tomlplusplus)
        find_package(tomlplusplus CONFIG REQUIRED)
    endif()

    add_library(takt_toml_config STATIC
        src/pipeline/pipeline_toml_config.cpp
        src/pipeline/pipeline_toml_dot.cpp)
    add_library(takt::toml_config ALIAS takt_toml_config)
    set_target_properties(takt_toml_config PROPERTIES
        EXPORT_NAME toml_config
        CXX_EXTENSIONS OFF)
    target_compile_features(takt_toml_config PUBLIC cxx_std_17)
    target_include_directories(takt_toml_config
        PUBLIC
            $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
            $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>)
    target_include_directories(takt_toml_config SYSTEM PRIVATE
        $<TARGET_PROPERTY:tomlplusplus::tomlplusplus,INTERFACE_INCLUDE_DIRECTORIES>)
    target_link_libraries(takt_toml_config PUBLIC takt::takt)
    takt_warnings(takt_toml_config)

    install(TARGETS takt_toml_config
        EXPORT TaktTomlTargets
        ARCHIVE DESTINATION "${CMAKE_INSTALL_LIBDIR}"
        INCLUDES DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}")
    install(EXPORT TaktTomlTargets
        FILE TaktTomlTargets.cmake
        NAMESPACE takt::
        DESTINATION "${TAKT_INSTALL_CMAKEDIR}")

    configure_package_config_file(
        "${CMAKE_CURRENT_SOURCE_DIR}/cmake/TaktTomlConfig.cmake.in"
        "${CMAKE_CURRENT_BINARY_DIR}/TaktTomlConfig.cmake"
        INSTALL_DESTINATION "${TAKT_INSTALL_CMAKEDIR}")
    write_basic_package_version_file(
        "${CMAKE_CURRENT_BINARY_DIR}/TaktTomlConfigVersion.cmake"
        VERSION "${PROJECT_VERSION}"
        COMPATIBILITY SameMinorVersion)
    install(FILES
        "${CMAKE_CURRENT_BINARY_DIR}/TaktTomlConfig.cmake"
        "${CMAKE_CURRENT_BINARY_DIR}/TaktTomlConfigVersion.cmake"
        DESTINATION "${TAKT_INSTALL_CMAKEDIR}")
endif()

if(TAKT_BUILD_EXAMPLES)
    add_subdirectory(examples)
endif()

if(TAKT_BUILD_TESTS)
    enable_testing()
    add_subdirectory(tests)
endif()
