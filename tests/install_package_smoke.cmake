foreach(required_var BUILD_DIR TEST_SOURCE_DIR TEST_BINARY_ROOT CONFIGURATION GENERATOR)
    if(NOT DEFINED ${required_var})
        message(FATAL_ERROR "Missing required variable: ${required_var}")
    endif()
endforeach()

file(REMOVE_RECURSE "${TEST_BINARY_ROOT}")
set(_install_prefix "${TEST_BINARY_ROOT}/prefix")
set(_consumer_binary_dir "${TEST_BINARY_ROOT}/consumer")

execute_process(
    COMMAND "${CMAKE_COMMAND}" --install "${BUILD_DIR}"
            --prefix "${_install_prefix}" --config "${CONFIGURATION}"
    RESULT_VARIABLE _install_result
    OUTPUT_VARIABLE _install_stdout
    ERROR_VARIABLE _install_stderr
)
if(NOT _install_result EQUAL 0)
    message(FATAL_ERROR
        "Installing Takt failed.\n${_install_stdout}\n${_install_stderr}")
endif()

set(_configure_command
    "${CMAKE_COMMAND}"
    -S "${TEST_SOURCE_DIR}"
    -B "${_consumer_binary_dir}"
    -G "${GENERATOR}"
    "-DCMAKE_PREFIX_PATH=${_install_prefix}"
)
if(CONFIGURATION)
    list(APPEND _configure_command "-DCMAKE_BUILD_TYPE=${CONFIGURATION}")
endif()
if(GENERATOR_PLATFORM)
    list(APPEND _configure_command -A "${GENERATOR_PLATFORM}")
endif()
if(GENERATOR_TOOLSET)
    list(APPEND _configure_command -T "${GENERATOR_TOOLSET}")
endif()

execute_process(
    COMMAND ${_configure_command}
    RESULT_VARIABLE _configure_result
    OUTPUT_VARIABLE _configure_stdout
    ERROR_VARIABLE _configure_stderr
)
if(NOT _configure_result EQUAL 0)
    message(FATAL_ERROR
        "Configuring the installed-package consumer failed.\n${_configure_stdout}\n${_configure_stderr}")
endif()

execute_process(
    COMMAND "${CMAKE_COMMAND}" --build "${_consumer_binary_dir}"
            --config "${CONFIGURATION}" --parallel
    RESULT_VARIABLE _build_result
    OUTPUT_VARIABLE _build_stdout
    ERROR_VARIABLE _build_stderr
)
if(NOT _build_result EQUAL 0)
    message(FATAL_ERROR
        "Building the installed-package consumer failed.\n${_build_stdout}\n${_build_stderr}")
endif()
