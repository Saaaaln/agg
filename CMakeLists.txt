cmake_minimum_required(VERSION 3.12)
project(aggregation)

set(CMAKE_CXX_STANDARD 11)

add_executable(aggregation main.cpp table.h line_item_parser.h line_item_parser.cpp proj_data.cpp proj_data.h hash_table.cpp hash_table.h)

INCLUDE_DIRECTORIES(/usr/local/include/)

set(CMAKE_CXX_FLAGS "-fopenmp")

target_link_libraries(aggregation -lsnappy)