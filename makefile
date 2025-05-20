# Compiler and flags
CXX = g++
CXXFLAGS = -Wall -Wextra -std=c++17 -O2

# Target executable
TARGET = blinkdb

# Source files
SRC = blinkDB.cpp

# Object files (derived from source files)
OBJ = $(SRC:.cpp=.o)

# Default rule to build the executable
all: $(TARGET)

# Rule to link the object files into the final executable
$(TARGET): $(OBJ)
	$(CXX) $(CXXFLAGS) -o $(TARGET) $(OBJ)

# Rule to compile the source files into object files
%.o: %.cpp
	$(CXX) $(CXXFLAGS) -c $< -o $@

# Clean up build artifacts
clean:
	rm -f $(OBJ) $(TARGET)

# Phony targets
.PHONY: all clean
