cmake_minimum_required(VERSION 3.20)

project(Takt VERSION 0.2.0 LANGUAGES CXX)

include(GNUInstallDirs)
include(CMakePackageConfigHelpers)

option(TAKT_BUILD_EXAMPLES "Build Takt examples" OFF)
option(TAKT_BUILD_TESTS "Build Takt tests" OFF)
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
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
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

if(TAKT_BUILD_EXAMPLES)
    add_subdirectory(examples)
endif()

if(TAKT_BUILD_TESTS)
    enable_testing()
    add_subdirectory(tests)
endif()
