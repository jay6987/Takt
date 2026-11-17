cmake_minimum_required(VERSION 3.25)
include("${CMAKE_CURRENT_LIST_DIR}/../../cmake/QualityChecks.cmake")
set(scratch "${CMAKE_CURRENT_BINARY_DIR}/quality-checker-fixtures")
file(MAKE_DIRECTORY "${scratch}")

function(expect_failure script)
    execute_process(COMMAND "${CMAKE_COMMAND}" -P "${script}"
        RESULT_VARIABLE result OUTPUT_VARIABLE output ERROR_VARIABLE error)
    if(result STREQUAL "0")
        message(FATAL_ERROR "Checker unexpectedly accepted ${script}")
    endif()
endfunction()

string(REPEAT "\n" 150 valid)
file(WRITE "${scratch}/size.h" "${valid}")
check_source_size("${scratch}/size.h")
file(APPEND "${scratch}/size.h" "x")
file(WRITE "${scratch}/reject-size.cmake"
    "include(\"${CMAKE_CURRENT_LIST_DIR}/../../cmake/QualityChecks.cmake\")\n"
    "check_source_size(\"${scratch}/size.h\")\n")
expect_failure("${scratch}/reject-size.cmake")

function(write_report lines branches)
    file(WRITE "${scratch}/coverage.json"
        "{\"data\":[{\"files\":[{\"filename\":\"fixture.cpp\",\"summary\":{"
        "\"lines\":{\"count\":100,\"covered\":${lines}},"
        "\"branches\":{\"count\":100,\"covered\":${branches}}}}]}]}")
endfunction()
write_report(80 80)
check_coverage("${scratch}/coverage.json")
file(WRITE "${scratch}/reject-coverage.cmake"
    "include(\"${CMAKE_CURRENT_LIST_DIR}/../../cmake/QualityChecks.cmake\")\n"
    "check_coverage(\"${scratch}/coverage.json\")\n")
write_report(79 80)
expect_failure("${scratch}/reject-coverage.cmake")
write_report(80 79)
expect_failure("${scratch}/reject-coverage.cmake")
write_report(101 80)
expect_failure("${scratch}/reject-coverage.cmake")
write_report(80 80)
file(WRITE "${scratch}/reject-missing.cmake"
    "include(\"${CMAKE_CURRENT_LIST_DIR}/../../cmake/QualityChecks.cmake\")\n"
    "check_coverage(\"${scratch}/coverage.json\" \"missing.cpp\")\n")
expect_failure("${scratch}/reject-missing.cmake")
file(WRITE "${scratch}/coverage.json"
    "{\"data\":[{\"files\":["
    "{\"filename\":\"first.cpp\",\"summary\":{\"lines\":{\"count\":10,\"covered\":8},"
    "\"branches\":{\"count\":10,\"covered\":8}}},"
    "{\"filename\":\"second.cpp\",\"summary\":{\"lines\":{\"count\":10,\"covered\":8},"
    "\"branches\":{\"count\":10,\"covered\":8}}}]}]}")
check_coverage("${scratch}/coverage.json" first.cpp second.cpp)
expect_failure("${scratch}/reject-missing.cmake")
file(REMOVE_RECURSE "${scratch}")
