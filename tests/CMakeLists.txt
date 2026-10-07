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

if(_takt_try_compile_target_type_was_defined)
    set(CMAKE_TRY_COMPILE_TARGET_TYPE "${_takt_previous_try_compile_target_type}")
else()
    unset(CMAKE_TRY_COMPILE_TARGET_TYPE)
endif()
