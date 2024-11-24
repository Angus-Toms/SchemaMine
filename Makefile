# Compiler and flags
CXX = g++
CXXFLAGS = -std=c++11 -I./src/include -I./duckdb  # Include src/include and duckdb directories

# Directories
BUILD_DIR = build
LIB_DIR = duckdb   # Directory where libduckdb.dylib is located

# Target
TARGET = $(BUILD_DIR)/run_program

# Ensure build directory exists
$(BUILD_DIR):
	mkdir -p $(BUILD_DIR)

# Build rule (now points to src/main.cpp)
$(TARGET): $(BUILD_DIR) src/main.cpp
	$(CXX) $(CXXFLAGS) src/main.cpp -o $(TARGET) -L$(LIB_DIR) -lduckdb

# Run rule with library path
run: $(TARGET)
	DYLD_LIBRARY_PATH=$(LIB_DIR) ./$(TARGET)

# Clean rule
clean:
	rm -rf $(BUILD_DIR)
