cmake_minimum_required(VERSION 3.20)

project(TaktInstalledPackageSmoke LANGUAGES CXX)

find_package(Takt 0.2 CONFIG REQUIRED)

add_executable(takt_package_consumer main.cpp)
target_link_libraries(takt_package_consumer PRIVATE takt::takt)
