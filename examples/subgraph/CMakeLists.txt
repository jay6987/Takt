add_executable(takt_subgraph_example
    main.cpp
    double_node.cpp
)
takt_warnings(takt_subgraph_example)

target_link_libraries(takt_subgraph_example
    PRIVATE
        takt::takt
)
