cmake_minimum_required(VERSION 3.10)
project(kv2nodefd CXX)

set(CMAKE_CXX_STANDARD 17)

add_executable(kvnode src/main.cpp)
target_link_libraries(kvnode PRIVATE pthread)

if(UNIX AND NOT APPLE)
  target_link_libraries(kvnode PRIVATE rt)  # clock_gettime
endif()
