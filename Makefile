# Compiler
CXX = g++

# Compiler flags
CXXFLAGS = -Wall -g -Iinterface/generated

# Source files
SRCS = src/server/keyValueStore.cpp src/server/sqlite_interface.cpp src/server/server.cpp

# Include generated proto files
PROTO_SRCS = interface/generated/*.cpp

# Object files
OBJS = $(SRCS:.cpp=.o) $(PROTO_SRCS:.cpp=.o)

# Target executable
TARGET = server

# Default rule
all: $(TARGET)

# Link the object files to create the executable
$(TARGET): $(OBJS)
	$(CXX) $(CXXFLAGS) -o $@ $^

# Compile source files to object files
%.o: %.cpp
	$(CXX) $(CXXFLAGS) -c $< -o $@

# Clean up the build
clean:
	rm -f $(OBJS) $(TARGET)

.PHONY: all clean
