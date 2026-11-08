add_executable(takt_pipe_example
    main.cpp
)
takt_warnings(takt_pipe_example)

target_link_libraries(takt_pipe_example
    PRIVATE
        takt::takt
)
