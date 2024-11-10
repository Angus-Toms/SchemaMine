cmake_minimum_required(VERSION 3.11)
project(DuckDBExample)

# Set C++ standard
set(CMAKE_CXX_STANDARD 11)

# Add executable
add_executable(run_program main.cpp)

# Specify DuckDB include directory
target_include_directories(run_program PRIVATE 
    ${PROJECT_SOURCE_DIR}/third_party/duckdb
)

# Link DuckDB and fmt libraries
target_link_libraries(run_program PRIVATE duckdb)
