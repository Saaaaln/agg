cmake_minimum_required(VERSION 2.8.11)
project(aggregation)

set(CMAKE_CXX_STANDARD 11)

add_definitions(-std=c++11)

add_executable(localagg main.cpp line_item_parser.h line_item_parser.cpp proj_data.cpp proj_data.h hash_table.cpp hash_table.h)

add_executable(coordinator coordinator.cpp net_buffer.h line_item_parser.h line_item_parser.cpp proj_data.cpp proj_data.h hash_table.cpp hash_table.h)

add_executable(worker workers.cpp net_buffer.h line_item_parser.h line_item_parser.cpp proj_data.cpp proj_data.h hash_table.cpp hash_table.h)

INCLUDE_DIRECTORIES(/usr/local/include/)

set(CMAKE_CXX_FLAGS "-fopenmp")

#target_link_libraries(localagg -lsnappy)
