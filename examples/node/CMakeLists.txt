add_executable(takt_node_example
    main.cpp
    double_node.cpp
)
takt_warnings(takt_node_example)

target_link_libraries(takt_node_example
    PRIVATE
        takt::takt
)
