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
    pipe_config_tests.cpp
    pipe_lifecycle_tests.cpp
    runtime_initialization_tests.cpp
    pipeline_builder_tests.cpp
    pipe_tests.cpp
    node_tests.cpp
    pipeline_contract_tests.cpp
    graphviz_tests.cpp
    subgraph_direct_tests.cpp
)

target_link_libraries(takt_smoke_test
    PRIVATE
        takt::takt
        GTest::gtest_main
)

gtest_discover_tests(takt_smoke_test)

get_directory_property(_takt_compile_options COMPILE_OPTIONS)
get_directory_property(_takt_link_options LINK_OPTIONS)
string(JOIN " " _takt_compile_flags ${_takt_compile_options})
string(JOIN " " _takt_link_flags ${_takt_link_options})
add_test(
    NAME TaktInstallPackageSmoke
    COMMAND ${CMAKE_COMMAND}
        "-DBUILD_DIR=${PROJECT_BINARY_DIR}"
        "-DTEST_SOURCE_DIR=${CMAKE_CURRENT_SOURCE_DIR}/package_consumer"
        "-DTEST_BINARY_ROOT=${CMAKE_CURRENT_BINARY_DIR}/package-consumer-smoke"
        "-DCONFIGURATION=$<CONFIG>"
        "-DGENERATOR=${CMAKE_GENERATOR}"
        "-DGENERATOR_PLATFORM=${CMAKE_GENERATOR_PLATFORM}"
        "-DGENERATOR_TOOLSET=${CMAKE_GENERATOR_TOOLSET}"
        "-DCXX_COMPILER=${CMAKE_CXX_COMPILER}"
        "-DCXX_FLAGS=${CMAKE_CXX_FLAGS} ${_takt_compile_flags}"
        "-DEXE_LINKER_FLAGS=${CMAKE_EXE_LINKER_FLAGS} ${_takt_link_flags}"
        -P "${CMAKE_CURRENT_SOURCE_DIR}/install_package_smoke.cmake"
)

set(_takt_try_compile_target_type_was_defined FALSE)
if(DEFINED CMAKE_TRY_COMPILE_TARGET_TYPE)
    set(_takt_try_compile_target_type_was_defined TRUE)
    set(_takt_previous_try_compile_target_type "${CMAKE_TRY_COMPILE_TARGET_TYPE}")
endif()
set(CMAKE_TRY_COMPILE_TARGET_TYPE STATIC_LIBRARY)

try_compile(
    public_api_control_compiles
    "${CMAKE_CURRENT_BINARY_DIR}/public_api_control"
    SOURCES "${CMAKE_CURRENT_SOURCE_DIR}/recordreplay_private_api_compile_test.cpp"
    CMAKE_FLAGS
        "-DCMAKE_CXX_STANDARD=17"
        "-DCMAKE_CXX_STANDARD_REQUIRED=ON"
        "-DINCLUDE_DIRECTORIES:STRING=${PROJECT_SOURCE_DIR}/include"
    COMPILE_DEFINITIONS "-DTAKT_TEST_PUBLIC_API_CONTROL"
    OUTPUT_VARIABLE public_api_control_output
)

if(NOT public_api_control_compiles)
    message(FATAL_ERROR
        "Record/replay API compile-test control failed:\n${public_api_control_output}")
endif()

function(expect_registry_binding_api_private test_name selector)
    try_compile(
        api_is_accessible
        "${CMAKE_CURRENT_BINARY_DIR}/${test_name}"
        SOURCES "${CMAKE_CURRENT_SOURCE_DIR}/recordreplay_private_api_compile_test.cpp"
        CMAKE_FLAGS
            "-DCMAKE_CXX_STANDARD=17"
            "-DCMAKE_CXX_STANDARD_REQUIRED=ON"
            "-DINCLUDE_DIRECTORIES:STRING=${PROJECT_SOURCE_DIR}/include"
        COMPILE_DEFINITIONS "-D${selector}"
    )

    if(api_is_accessible)
        message(FATAL_ERROR "Registry API ${test_name} must be private")
    endif()
endfunction()

expect_registry_binding_api_private(set_record TAKT_TEST_SET_RECORD_PRIVATE)
expect_registry_binding_api_private(set_replay TAKT_TEST_SET_REPLAY_PRIVATE)
expect_registry_binding_api_private(clear_record TAKT_TEST_CLEAR_RECORD_PRIVATE)
expect_registry_binding_api_private(clear_replay TAKT_TEST_CLEAR_REPLAY_PRIVATE)

function(check_pipe_config_compilation selector expected)
    try_compile(
        config_compiles
        "${CMAKE_CURRENT_BINARY_DIR}/pipe_config_${selector}"
        SOURCES "${CMAKE_CURRENT_SOURCE_DIR}/pipe_config_compile_test.cpp"
        CMAKE_FLAGS
            "-DCMAKE_CXX_STANDARD=17"
            "-DCMAKE_CXX_STANDARD_REQUIRED=ON"
            "-DINCLUDE_DIRECTORIES:STRING=${PROJECT_SOURCE_DIR}/include"
        COMPILE_DEFINITIONS "-DTAKT_TEST_${selector}"
        OUTPUT_VARIABLE config_compile_output
    )

    if(expected AND NOT config_compiles)
        message(FATAL_ERROR
            "Pipe configuration compile-test control failed:\n${config_compile_output}")
    elseif(NOT expected AND config_compiles)
        message(FATAL_ERROR "Pipe configuration ${selector} must not compile")
    endif()
endfunction()

check_pipe_config_compilation(CONFIG_CONTROL TRUE)
foreach(selector IN ITEMS
    PRODUCER_BARE
    CONSUMER_BARE
    PRODUCER_BARE_CONCURRENCY
    PRODUCER_BARE_FRAMES
    CONSUMER_BARE_CONCURRENCY
    CONSUMER_BARE_FRAMES
    CONSUMER_BARE_OVERLAP
    PRODUCER_SWAPPED
    CONSUMER_SWAPPED_CONCURRENCY
    CONSUMER_SWAPPED_OVERLAP
    PRODUCER_READ_FRAMES
    CONSUMER_WRITE_FRAMES
)
    check_pipe_config_compilation("${selector}" FALSE)
endforeach()

if(_takt_try_compile_target_type_was_defined)
    set(CMAKE_TRY_COMPILE_TARGET_TYPE "${_takt_previous_try_compile_target_type}")
else()
    unset(CMAKE_TRY_COMPILE_TARGET_TYPE)
endif()
