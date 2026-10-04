add_executable(takt_recordreplay_example
    main.cpp
)

target_link_libraries(takt_recordreplay_example
    PRIVATE
        takt::takt
)
