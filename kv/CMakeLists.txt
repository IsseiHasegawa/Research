cmake_minimum_required(VERSION 3.16)
project(kvnode CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

include(FetchContent)
FetchContent_Declare(json
  URL https://github.com/nlohmann/json/releases/download/v3.11.3/json.tar.xz
)
FetchContent_MakeAvailable(json)

add_executable(kvnode
  src/main.cpp
  src/node.cpp
  src/store.cpp
  src/util.cpp
)

target_include_directories(kvnode PRIVATE
  ${CMAKE_CURRENT_SOURCE_DIR}/src
  ${CMAKE_CURRENT_SOURCE_DIR}/vendor
)

find_package(Threads REQUIRED)
target_link_libraries(kvnode PRIVATE Threads::Threads nlohmann_json::nlohmann_json)