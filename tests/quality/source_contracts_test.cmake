cmake_minimum_required(VERSION 3.25)
if(NOT CHECKER OR NOT CLANG OR NOT FORMATTER)
    message(FATAL_ERROR "CHECKER, CLANG and FORMATTER are required")
endif()
set(scratch "${CMAKE_CURRENT_BINARY_DIR}/build/source-contract-fixtures")
file(MAKE_DIRECTORY "${scratch}/modules")
set(source "${scratch}/modules/example.cpp")
file(WRITE "${scratch}/compile_commands.json"
    "[{\"directory\":\"${scratch}\",\"file\":\"${source}\","
    "\"arguments\":[\"${CLANG}\",\"-std=c++20\",\"-c\",\"${source}\"]}]")

function(check_case name)
    math(EXPR last "${ARGC} - 1")
    math(EXPR end "${last} - 1")
    set(content "")
    foreach(index RANGE 1 ${end})
        string(APPEND content "${ARGV${index}}")
    endforeach()
    set(diagnostic "${ARGV${last}}")
    file(WRITE "${source}" "${content}")
    execute_process(COMMAND "${CHECKER}" "${scratch}" "${scratch}"
        RESULT_VARIABLE result OUTPUT_VARIABLE output ERROR_VARIABLE error)
    if(diagnostic STREQUAL "")
        if(NOT result STREQUAL "0")
            message(FATAL_ERROR "${name} rejected: ${output}\n${error}")
        endif()
    elseif(result STREQUAL "0" OR
        NOT "${output}${error}" MATCHES "${diagnostic}")
        message(FATAL_ERROR "${name} missing diagnosis: ${output}\n${error}")
    endif()
endfunction()

set(header "// Exercises source documentation and layout contracts.\n\n")
set(function "// Adds two values for the contract fixture.\n")
check_case(valid "${header}${function}int add(int a,\n int b) {return a+b;}\n" "")
check_case(missing_file "${function}int add() {return 1;}\n"
    "missing function comment")
check_case(no_header "#include <cstddef>\n${function}int add() {return 1;}\n"
    "missing file responsibility comment")
check_case(missing_function "${header}int add() {return 1;}\n"
    "missing function comment")
check_case(parameters "${header}${function}int add(int a, int b) {return a+b;}\n"
    "parameters must occupy separate lines")
check_case(first_newline "${header}${function}int add(\n int a,\n int b)"
    "{return a+b;}\n" "")
check_case(redeclaration "${header}${function}int add(int a);\n"
    "int add(int a) {return a;}\n" "")
check_case(overload "${header}${function}int add(int a);\n"
    "int add(float a) {return static_cast<int>(a);}\n"
    "missing function comment")
check_case(unrelated "${header}${function}int value = 1;\n"
    "int add() {return value;}\n" "missing function comment")
check_case(lambda "${header}auto value = [](int a, int b) {return a+b;};\n" "")
check_case(template "${header}${function}template <typename T>\n"
    "T add(T a,\n T b) {return a+b;}\n" "")
check_case(defaulted "${header}struct Item {\n"
    "// Creates an empty fixture.\nItem() = default;\n"
    "// Disallows copying the fixture.\nItem(const Item &) = delete;\n};\n" "")
check_case(default_missing "${header}struct Item {Item() = default;};\n"
    "missing function comment")
check_case(pointer "${header}${function}int add(int (*op)(int, int),\n"
    "int b) {return op(b,b);}\n" "")
check_case(void "${header}${function}void add(void) {}\n" "")
check_case(operators "${header}struct Item {\n"
    "// Releases the fixture without external resources.\n~Item() = default;\n"
    "// Provides the fixed comparison fixture value.\n"
    "operator int() const {return 1;}\n"
    "// Copies the fixture state.\n"
    "Item &operator=(const Item &) = default;\n};\n" "")
check_case(block_comment "${header}/* Returns a fixture value. */\n"
    "int add() {return 1;}\n" "")
check_case(punctuation "${header}// ----\nint add() {return 1;}\n"
    "missing function comment")
check_case(multiline_parameter "${header}${function}"
    "int add(int a =\n 1, int b = 2) {return a+b;}\n"
    "parameters must occupy separate lines")
check_case(test_macro "${header}#define TEST(a,b) void a##b()\n"
    "// Checks an authored macro-generated test entry point.\n"
    "TEST(Suite, Case) {}\n" "")
check_case(test_missing "${header}#define TEST(a,b) void a##b()\n"
    "TEST(Suite, Case) {}\n" "undocumented test case")
check_case(raw_literal "${header}const char *value = R\"(\n"
    "TEST(Suite, Case) {}\n)\";\n" "")
string(REPEAT "x" 77 boundary)
check_case(columns80 "${header}// ${boundary}\n" "")
check_case(columns81 "${header}// ${boundary}x\n" "line exceeds 80 columns")
check_case(tabs "${header}\t// ${boundary}\n" "line exceeds 80 columns")

file(WRITE "${source}" "${header}int add(int a, int b);\n")
execute_process(COMMAND "${FORMATTER}" "${source}"
    OUTPUT_VARIABLE formatted RESULT_VARIABLE result)
if(NOT result STREQUAL "0" OR NOT formatted MATCHES "int a,\n[ ]+int b")
    message(FATAL_ERROR "Formatter did not split short declaration parameters")
endif()
set(orphan "${scratch}/modules/orphan.h")
file(WRITE "${orphan}" "// Exposes a fixture not yet included by the build.\n\n"
    "// Returns a fixture value.\nint missing();\n")
file(WRITE "${source}" "${header}${function}int add() {return 1;}\n")
execute_process(COMMAND "${CHECKER}" "${scratch}" "${scratch}" --complete
    RESULT_VARIABLE result OUTPUT_VARIABLE output ERROR_VARIABLE error)
if(result STREQUAL "0" OR
    NOT "${output}${error}" MATCHES "source not reached by the build AST")
    message(FATAL_ERROR "Unparsed header escaped complete source checks")
endif()
check_case(included_header "${header}#include \"orphan.h\"\n" "")
file(WRITE "${orphan}" "// Exposes a fixture with undocumented declarations.\n\n"
    "int missing();\n")
check_case(header_missing "${header}#include \"orphan.h\"\n"
    "missing function comment")

file(MAKE_DIRECTORY "${scratch}/examples")
set(entry "${scratch}/examples/main.cpp")
file(WRITE "${entry}" "${header}int main() {return 0;}\n")
file(WRITE "${source}" "${header}// Runs the documented entry point.\n"
    "int main() {return 0;}\n")
file(WRITE "${scratch}/compile_commands.json"
    "[{\"directory\":\"${scratch}\",\"file\":\"${source}\","
    "\"arguments\":[\"${CLANG}\",\"-c\",\"${source}\"]},"
    "{\"directory\":\"${scratch}\",\"file\":\"${entry}\","
    "\"arguments\":[\"${CLANG}\",\"-c\",\"${entry}\"]}]")
execute_process(COMMAND "${CHECKER}" "${scratch}" "${scratch}"
    RESULT_VARIABLE result OUTPUT_VARIABLE output ERROR_VARIABLE error)
if(result STREQUAL "0" OR
    NOT "${output}${error}" MATCHES "examples/main.cpp.*missing function comment")
    message(FATAL_ERROR "Independent main escaped documentation: ${output}\n${error}")
endif()
file(REMOVE_RECURSE "${scratch}")
