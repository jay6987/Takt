add_executable(takt_subgraph_example
    main.cpp
)

target_link_libraries(takt_subgraph_example
    PRIVATE
        takt::takt
)
