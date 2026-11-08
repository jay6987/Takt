function(takt_add_source_checker target)
    find_path(TAKT_CLANG_INCLUDE clang-c/Index.h
        HINTS "$ENV{LYNX_LLVM_ROOT}/include" "$ENV{TAKT_LLVM_ROOT}/include"
        /opt/homebrew/opt/llvm/include /usr/lib/llvm-23/include REQUIRED)
    find_library(TAKT_CLANG_LIBRARY NAMES clang libclang
        HINTS "$ENV{LYNX_LLVM_ROOT}/lib" "$ENV{TAKT_LLVM_ROOT}/lib"
        /opt/homebrew/opt/llvm/lib /usr/lib/llvm-23/lib REQUIRED)
    add_executable(${target}
        "${CMAKE_CURRENT_FUNCTION_LIST_DIR}/../tools/source_policy.cpp"
        "${CMAKE_CURRENT_FUNCTION_LIST_DIR}/../tools/source_contracts.cpp")
    target_compile_features(${target} PRIVATE cxx_std_20)
    target_include_directories(${target} SYSTEM PRIVATE ${TAKT_CLANG_INCLUDE})
    target_link_libraries(${target} PRIVATE ${TAKT_CLANG_LIBRARY})
    set_target_properties(${target} PROPERTIES CXX_EXTENSIONS OFF)
endfunction()
