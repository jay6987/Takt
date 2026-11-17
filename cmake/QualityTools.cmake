function(takt_add_source_checker target)
    find_path(TAKT_CLANG_INCLUDE clang-c/Index.h
        HINTS "$ENV{LYNX_LLVM_ROOT}/include" "$ENV{TAKT_LLVM_ROOT}/include"
        /opt/homebrew/opt/llvm/include /usr/lib/llvm-23/include REQUIRED)
    find_library(TAKT_CLANG_LIBRARY NAMES clang libclang
        HINTS "$ENV{LYNX_LLVM_ROOT}/lib" "$ENV{TAKT_LLVM_ROOT}/lib"
        /opt/homebrew/opt/llvm/lib /usr/lib/llvm-23/lib REQUIRED)
    add_executable(${target}
        "${CMAKE_CURRENT_FUNCTION_LIST_DIR}/../tools/source_policy.cpp"
        "${CMAKE_CURRENT_FUNCTION_LIST_DIR}/../tools/source_policy_ast.cpp"
        "${CMAKE_CURRENT_FUNCTION_LIST_DIR}/../tools/source_policy_body.cpp"
        "${CMAKE_CURRENT_FUNCTION_LIST_DIR}/../tools/source_policy_compile.cpp"
        "${CMAKE_CURRENT_FUNCTION_LIST_DIR}/../tools/source_policy_database.cpp"
        "${CMAKE_CURRENT_FUNCTION_LIST_DIR}/../tools/source_contracts.cpp"
        "${CMAKE_CURRENT_FUNCTION_LIST_DIR}/../tools/source_contracts_declarations.cpp"
        "${CMAKE_CURRENT_FUNCTION_LIST_DIR}/../tools/source_contracts_helpers.cpp"
        "${CMAKE_CURRENT_FUNCTION_LIST_DIR}/../tools/source_contracts_macros.cpp"
        "${CMAKE_CURRENT_FUNCTION_LIST_DIR}/../tools/source_contracts_types.cpp")
    target_compile_features(${target} PRIVATE cxx_std_20)
    target_include_directories(${target} SYSTEM PRIVATE ${TAKT_CLANG_INCLUDE})
    target_link_libraries(${target} PRIVATE ${TAKT_CLANG_LIBRARY})
    set_target_properties(${target} PROPERTIES CXX_EXTENSIONS OFF)
    find_program(TAKT_CLANG_FORMAT
        NAMES clang-format-23 clang-format
        HINTS "$ENV{LYNX_LLVM_ROOT}/bin" "$ENV{TAKT_LLVM_ROOT}/bin"
        /opt/homebrew/opt/llvm/bin /usr/lib/llvm-23/bin REQUIRED)
    if(TAKT_BUILD_TESTS)
        add_test(
            NAME TaktReadabilityFixtures
            COMMAND ${CMAKE_COMMAND}
                "-DCHECKER=$<TARGET_FILE:${target}>"
                "-DCLANG=${CMAKE_CXX_COMPILER}"
                "-DFORMATTER=${TAKT_CLANG_FORMAT}"
                -P
                "${CMAKE_CURRENT_FUNCTION_LIST_DIR}/../tests/quality/source_contracts_test.cmake"
        )
        add_test(
            NAME TaktQualityHelperFixtures
            COMMAND ${CMAKE_COMMAND}
                -P
                "${CMAKE_CURRENT_FUNCTION_LIST_DIR}/../tests/quality/checks_test.cmake"
        )
    endif()
endfunction()
