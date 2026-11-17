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

function(check_contracts_case name content expected)
    file(WRITE "${source}" "${content}")
    execute_process(COMMAND "${CHECKER}" "${scratch}" "${scratch}"
        --contracts-only RESULT_VARIABLE result OUTPUT_VARIABLE output
        ERROR_VARIABLE error)
    if(expected STREQUAL "")
        if(NOT result STREQUAL "0")
            message(FATAL_ERROR
                "${name} rejected: ${output}\n${error}")
        endif()
    elseif(result STREQUAL "0" OR
        NOT "${output}${error}" MATCHES "${expected}")
        message(FATAL_ERROR
            "${name} missing diagnosis: ${output}\n${error}")
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
check_case(defaulted "${header}/// Stores a reusable contract fixture.\n"
    "struct Item {\n"
    "/// Creates an empty fixture.\nItem() = default;\n"
    "/// Disallows copying the fixture.\nItem(const Item &) = delete;\n};\n" "")
check_case(default_missing "${header}struct Item {Item() = default;};\n"
    "missing function comment")
check_case(pointer "${header}${function}int add(int (*op)(int, int),\n"
    "int b) {return op(b,b);}\n" "")
check_case(void "${header}${function}void add(void) {}\n" "")
check_case(operators "${header}/// Stores a reusable operator fixture.\n"
    "struct Item {\n"
    "/// Releases the fixture without external resources.\n~Item() = default;\n"
    "/// Provides the fixed comparison fixture value.\n"
    "operator int() const {return 1;}\n"
    "/// Copies the fixture state.\n"
    "Item &operator=(const Item &) = default;\n};\n" "")
check_case(block_comment "${header}/* Returns a fixture value. */\n"
    "int add() {return 1;}\n" "")
check_case(punctuation "${header}// ----\nint add() {return 1;}\n"
    "missing function comment")
check_case(multiline_parameter "${header}${function}"
    "int add(int a =\n 1, int b = 2) {return a+b;}\n"
    "parameters must occupy separate lines")
check_case(test_macro "${header}#define TEST(a,b) void a##b()\n"
    "/// Checks an authored macro-generated test entry point.\n"
    "TEST(Suite, Case) {}\n" "")
check_case(test_missing "${header}#define TEST(a,b) void a##b()\n"
    "TEST(Suite, Case) {}\n" "undocumented test case")
check_case(raw_literal "${header}const char *value = R\"(\n"
    "TEST(Suite, Case) {}\n)\";\n" "")

set(forty_lines "${header}${function}int body() {\n")
foreach(index RANGE 1 37)
    string(APPEND forty_lines "    static_cast<void>(${index});\n")
endforeach()
string(APPEND forty_lines "    return 0;\n}\n")
check_case(body_40 "${forty_lines}" "")

set(forty_one_lines "${header}${function}int body() {\n")
foreach(index RANGE 1 38)
    string(APPEND forty_one_lines "    static_cast<void>(${index});\n")
endforeach()
string(APPEND forty_one_lines "    return 0;\n}\n")
check_case(body_41 "${forty_one_lines}"
    "callable body has 41 effective non-comment lines")

foreach(callable IN ITEMS template lambda)
    foreach(limit IN ITEMS 40 41)
        if(callable STREQUAL "template")
            set(boundary_body "${header}${function}template <typename T>\n")
            string(APPEND boundary_body "int body() {\n")
        else()
            set(boundary_body "${header}auto body = []() {\n")
        endif()
        math(EXPR statements "${limit} - 3")
        foreach(index RANGE 1 ${statements})
            string(APPEND boundary_body "    static_cast<void>(${index});\n")
        endforeach()
        string(APPEND boundary_body "    return 0;\n}")
        if(callable STREQUAL "lambda")
            string(APPEND boundary_body ";")
        endif()
        string(APPEND boundary_body "\n")
        if(limit EQUAL 40)
            check_case("${callable}_40" "${boundary_body}" "")
        else()
            check_case("${callable}_41" "${boundary_body}"
                "callable body has 41 effective non-comment lines")
        endif()
    endforeach()
endforeach()

string(REPLACE "    static_cast<void>(1);\n"
    "\n    // Ignored comment-only line.\n    /* Ignored block comment.\n       Still an ignored comment. */\n\n    static_cast<void>(1); // Code still counts.\n"
    commented_body "${forty_lines}")
check_case(body_40_comments_and_blanks "${commented_body}" "")

set(raw_body "${header}${function}int body() {\n")
string(APPEND raw_body "    const char *text = R\"tag(\n")
foreach(index RANGE 1 40)
    string(APPEND raw_body "raw line ${index}\n")
endforeach()
string(APPEND raw_body ")tag\";\n    return 0;\n}\n")
check_case(raw_string_body "${raw_body}"
    "callable body has .* effective non-comment lines")

set(macro_body "${header}#define MULTILINE_BODY \\\n")
string(APPEND macro_body "    do { \\\n")
string(APPEND macro_body "        static_cast<void>(0); \\\n")
string(APPEND macro_body "    } while (false)\n"
    "// Uses a multiline macro as one authored body statement.\n"
    "int body() {\n    MULTILINE_BODY;\n    return 0;\n}\n")
check_case(macro_body "${macro_body}" "")

set(try_body "${header}${function}int body() try {\n")
foreach(index RANGE 1 20)
    string(APPEND try_body "    static_cast<void>(${index});\n")
endforeach()
string(APPEND try_body "    return 0;\n} catch (...) {\n")
foreach(index RANGE 21 40)
    string(APPEND try_body "    static_cast<void>(${index});\n")
endforeach()
string(APPEND try_body "    return 1;\n}\n")
check_case(function_try_body "${try_body}"
    "callable body has .* effective non-comment lines")

check_case(documented_nested_template
    "${header}/** Stores a value and its nested representation. */\n"
    "template <typename T>\nstruct Outer {\n"
    "    /// Describes the nested representation.\n"
    "    struct Inner {};\n};\n" "")
check_case(undocumented_nested_class
    "${header}/// Documents only the enclosing class.\n"
    "class Outer { class Inner {}; };\n"
    "class/struct definition needs Doxygen documentation")
check_case(template_owner_does_not_document_nested_type
    "${header}/// Documents only the enclosing template.\n"
    "template <typename T>\nstruct Outer { struct Inner {}; };\n"
    "class/struct definition needs Doxygen documentation")
check_case(nested_docs_do_not_document_template_owner
    "${header}template <typename T>\nstruct Outer {\n"
    "/// Documents only the nested type.\nstruct Inner {};\n};\n"
    "class/struct definition needs Doxygen documentation")
check_case(undocumented_type "${header}struct Missing {};\n"
    "class/struct definition needs Doxygen documentation")
check_case(ordinary_type_comment "${header}// Not a Doxygen description.\n"
    "struct Missing {};\n"
    "class/struct definition needs Doxygen documentation")
check_case(documented_type_specialization
    "${header}/// Describes the primary type template.\n"
    "template <typename T>\nstruct Box {};\n"
    "/// Describes the explicit specialization.\n"
    "template <>\nstruct Box<int> {};\n" "")

string(REPEAT "x" 77 boundary)
check_case(columns80 "${header}// ${boundary}\n" "")
check_case(columns81 "${header}// ${boundary}x\n" "line exceeds 80 columns")
check_case(tabs "${header}\t// ${boundary}\n" "line exceeds 80 columns")

set(size_150 "${header}${function}int value() { return 0; }\n")
foreach(index RANGE 1 146)
    string(APPEND size_150 "// filler line ${index}\n")
endforeach()
check_case(file_150 "${size_150}" "")
check_case(file_151 "${size_150}// final line\n"
    "physical lines exceeds 150")
set(contracts_only_large "")
foreach(index RANGE 1 160)
    string(APPEND contracts_only_large "// filler line ${index}\n")
endforeach()
set(contracts_only_source
    "${header}${function}int body() {\n    return 0;\n}\n"
    "struct Undocumented {};\n${contracts_only_large}")
check_contracts_case(contracts_only_ignores_physical_and_body_caps
    "${contracts_only_source}" "")
check_contracts_case(contracts_only_keeps_callable_docs
    "${header}int body() { return 0; }\n" "missing function comment")
check_contracts_case(contracts_only_keeps_parameter_layout
    "${header}${function}int add(int a, int b) { return a + b; }\n"
    "parameters must occupy separate lines")

file(WRITE "${source}" "${header}int add(int a, int b);\n")
execute_process(COMMAND "${FORMATTER}" "${source}"
    OUTPUT_VARIABLE formatted RESULT_VARIABLE result)
if(NOT result STREQUAL "0" OR NOT formatted MATCHES "int a,\n[ ]+int b")
    message(FATAL_ERROR "Formatter did not split short declaration parameters")
endif()

set(orphan "${scratch}/modules/orphan.h")
file(WRITE "${orphan}" "// Exposes a fixture not yet included by the build.\n\n"
    "/// Returns a fixture value.\nint missing();\n")
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

file(REMOVE "${orphan}")
file(WRITE "${source}" "${header}${function}int add() {return 1;}\n")
execute_process(
    COMMAND "${CHECKER}" "${scratch}" "${scratch}" --complete "${scratch}"
        --contracts-only
    RESULT_VARIABLE result OUTPUT_VARIABLE output ERROR_VARIABLE error)
if(NOT result STREQUAL "0")
    message(FATAL_ERROR
        "Complete contracts-only invocation failed: ${output}\n${error}")
endif()

check_case(parse_error "${header}int broken( {\n" "error:")
check_case(macro_expansion "${header}#define RETURN_ZERO return 0\n"
    "/// Uses a source macro in an authored body.\n"
    "int value() { RETURN_ZERO; }\n" "")

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
