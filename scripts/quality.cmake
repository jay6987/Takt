cmake_minimum_required(VERSION 3.25)
get_filename_component(root "${CMAKE_CURRENT_LIST_DIR}/.." ABSOLUTE)
include("${root}/cmake/QualityChecks.cmake")

function(run)
    message(STATUS "Running: ${ARGV}")
    execute_process(COMMAND ${ARGV} WORKING_DIRECTORY "${root}"
        RESULT_VARIABLE result)
    if(NOT result STREQUAL "0")
        message(FATAL_ERROR "Quality command failed (${result}): ${ARGV}")
    endif()
endfunction()

set(hints "$ENV{TAKT_LLVM_ROOT}/bin" "$ENV{LYNX_LLVM_ROOT}/bin"
    /opt/homebrew/opt/llvm/bin /usr/lib/llvm-23/bin)
foreach(tool IN ITEMS clang++ clang-format llvm-cov llvm-profdata)
    string(REPLACE "-" "_" variable "${tool}")
    string(REPLACE "+" "x" variable "${variable}")
    find_program(${variable} NAMES "${tool}-23" "${tool}"
        HINTS ${hints} REQUIRED)
    execute_process(COMMAND "${${variable}}" --version
        OUTPUT_VARIABLE version RESULT_VARIABLE result)
    if(NOT result EQUAL 0 OR NOT version MATCHES "version 23\\.")
        message(FATAL_ERROR "${tool} must be LLVM 23: ${version}")
    endif()
endforeach()
find_program(ctest NAMES ctest REQUIRED)
set(platform_compilers)
if(CMAKE_HOST_APPLE)
    find_program(xcrun NAMES xcrun REQUIRED)
    execute_process(COMMAND "${xcrun}" --sdk macosx --show-sdk-path
        RESULT_VARIABLE sdk_result OUTPUT_VARIABLE macos_sdk
        ERROR_VARIABLE sdk_error OUTPUT_STRIP_TRAILING_WHITESPACE)
    if(NOT sdk_result STREQUAL "0" OR NOT IS_DIRECTORY "${macos_sdk}")
        message(FATAL_ERROR "Cannot resolve macOS SDK: ${sdk_error}")
    endif()
    list(APPEND platform_compilers "-DCMAKE_OSX_SYSROOT=${macos_sdk}")
endif()
set(sources)
foreach(directory IN ITEMS include src tests examples tools)
    foreach(extension IN ITEMS
        cpp cc cxx c++ C h hh hpp hxx ipp tpp inl mm)
        file(GLOB_RECURSE found LIST_DIRECTORIES false
            "${root}/${directory}/*.${extension}")
        list(APPEND sources ${found})
    endforeach()
endforeach()
list(REMOVE_DUPLICATES sources)
foreach(source IN LISTS sources)
    run("${clang_format}" --dry-run --Werror "${source}")
endforeach()
set(dependencies)
if(FETCHCONTENT_SOURCE_DIR_GOOGLETEST)
    list(APPEND dependencies
        "-DFETCHCONTENT_SOURCE_DIR_GOOGLETEST=${FETCHCONTENT_SOURCE_DIR_GOOGLETEST}")
endif()
set(build "${root}/build/quality")
set(ENV{LLVM_PROFILE_FILE} "${build}/control-%p.profraw")
run("${CMAKE_COMMAND}" -S "${root}" -B "${build}"
    "-DCMAKE_CXX_COMPILER=${clangxx}" -DCMAKE_BUILD_TYPE=Debug
    ${platform_compilers}
    -DCMAKE_EXPORT_COMPILE_COMMANDS=ON
    -DTAKT_BUILD_TESTS=ON -DTAKT_BUILD_EXAMPLES=ON
    -DTAKT_BUILD_QUALITY_TOOLS=ON -DTAKT_WARNINGS_AS_ERRORS=ON
    -DTAKT_COVERAGE=ON -DTAKT_SANITIZER=none ${dependencies})
run("${CMAKE_COMMAND}" --build "${build}" --parallel)
file(REMOVE_RECURSE "${build}/profiles")
file(MAKE_DIRECTORY "${build}/profiles")
run("${CMAKE_COMMAND}" -E env
    "LLVM_PROFILE_FILE=${build}/profiles/%p-%m.profraw"
    "${ctest}" --test-dir "${build}" --output-on-failure --timeout 60)
set(consumer "${build}/tests/package-consumer-smoke/consumer")
run("${build}/takt_source_policy" "${build}" "${root}"
    --complete "${consumer}")
file(GLOB profiles "${build}/profiles/*.profraw")
if(NOT profiles)
    message(FATAL_ERROR "No instrumentation profiles were produced")
endif()
run("${llvm_profdata}" merge -sparse ${profiles}
    -o "${build}/coverage.profdata")
file(GLOB_RECURSE production LIST_DIRECTORIES false
    "${root}/src/*.cpp" "${root}/include/*.h")
execute_process(COMMAND "${llvm_cov}" export
    -object "${build}/tests/takt_smoke_test"
    -object "${consumer}/takt_package_consumer"
    "-instr-profile=${build}/coverage.profdata"
    -summary-only --sources ${production}
    OUTPUT_FILE "${build}/coverage.json" RESULT_VARIABLE result)
if(NOT result EQUAL 0)
    message(FATAL_ERROR "LLVM coverage export failed: ${result}")
endif()
check_coverage("${build}/coverage.json" ${production})

file(STRINGS "${build}/CMakeCache.txt" dependency_entry
    REGEX "^FETCHCONTENT_SOURCE_DIR_GOOGLETEST:[^=]*=")
string(REGEX REPLACE "^[^=]*=" "" dependency_source "${dependency_entry}")
if(dependency_source STREQUAL "")
    set(dependency_source "${build}/_deps/googletest-src")
endif()
set(dependencies "-DFETCHCONTENT_SOURCE_DIR_GOOGLETEST=${dependency_source}")
foreach(kind IN ITEMS address-undefined thread)
    set(sanitizer_build "${root}/build/${kind}")
    run("${CMAKE_COMMAND}" -S "${root}" -B "${sanitizer_build}"
        "-DCMAKE_CXX_COMPILER=${clangxx}" -DCMAKE_BUILD_TYPE=Debug
        ${platform_compilers}
        -DTAKT_BUILD_TESTS=ON -DTAKT_BUILD_EXAMPLES=ON
        -DTAKT_BUILD_QUALITY_TOOLS=OFF -DTAKT_WARNINGS_AS_ERRORS=ON
        -DTAKT_COVERAGE=OFF "-DTAKT_SANITIZER=${kind}" ${dependencies})
    run("${CMAKE_COMMAND}" --build "${sanitizer_build}" --parallel)
    run("${CMAKE_COMMAND}" -E env ASAN_OPTIONS=halt_on_error=1
        UBSAN_OPTIONS=halt_on_error=1:print_stacktrace=1
        TSAN_OPTIONS=halt_on_error=1
        "${ctest}" --test-dir "${sanitizer_build}"
        --output-on-failure --timeout 60)
endforeach()
message(STATUS "All Takt quality gates passed")
