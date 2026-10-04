add_executable(takt_node_example
    main.cpp
)

target_link_libraries(takt_node_example
    PRIVATE
        takt::takt
)
