add_executable(takt_pipe_example
    main.cpp
)

target_link_libraries(takt_pipe_example
    PRIVATE
        takt::takt
)
