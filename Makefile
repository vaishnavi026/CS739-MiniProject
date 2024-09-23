# Compiler and flags
CC = g++
CFLAGS = -Wall -g

# Directories
CLIENT_DIR = client
SERVER_DIR = src

# Target binaries
CLIENT_BIN = client_app
SERVER_BIN = server_app

# Source files
CLIENT_SRC = $(wildcard $(CLIENT_DIR)/*.cpp)
SERVER_SRC = $(wildcard $(SERVER_DIR)/*.cpp)

# Object files
CLIENT_OBJ = $(CLIENT_SRC:.cpp=.o)
SERVER_OBJ = $(SERVER_SRC:.cpp=.o)

# Default target
all: client server

# Build client binary
client: $(CLIENT_OBJ)
	$(CC) $(CFLAGS) -o $(CLIENT_BIN) $(CLIENT_OBJ)

# Build server binary
server: $(SERVER_OBJ)
	$(CC) $(CFLAGS) -o $(SERVER_BIN) $(SERVER_OBJ)

# Clean up object files and binaries
clean:
	rm -f $(CLIENT_OBJ) $(SERVER_OBJ) $(CLIENT_BIN) $(SERVER_BIN)

# Rule for compiling client source files
$(CLIENT_DIR)/%.o: $(CLIENT_DIR)/%.cpp
	$(CC) $(CFLAGS) -c $< -o $@

# Rule for compiling server source files
$(SERVER_DIR)/%.o: $(SERVER_DIR)/%.cpp
	$(CC) $(CFLAGS) -c $< -o $@

