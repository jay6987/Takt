add_executable(takt_basic_example
    main.cpp
)

target_link_libraries(takt_basic_example
    PRIVATE
        takt::takt
)
