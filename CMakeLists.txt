cmake_minimum_required(VERSION 3.0)
project(CSV)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED True)
SET(INCLUDE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/include)
aux_source_directory(./src SRC)
add_executable(csv ${SRC})
target_include_directories(csv PUBLIC ${INCLUDE_DIR})


