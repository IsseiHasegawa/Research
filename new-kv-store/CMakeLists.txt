cmake_minimum_required(VERSION 3.10)
project(distributed_kv_store CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# ─── Node binary ───────────────────────────────────────────────────────────────
add_executable(kvnode src/main.cpp)
target_include_directories(kvnode PRIVATE src)
target_link_libraries(kvnode PRIVATE pthread)

# ─── Workload generator binary ─────────────────────────────────────────────────
add_executable(kv_workload src/client/workload.cpp)
target_include_directories(kv_workload PRIVATE src)
target_link_libraries(kv_workload PRIVATE pthread)

# macOS does not need -lrt; Linux does for clock_gettime in older glibc
if(UNIX AND NOT APPLE)
    target_link_libraries(kvnode PRIVATE rt)
    target_link_libraries(kv_workload PRIVATE rt)
endif()
