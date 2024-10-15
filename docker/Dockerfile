FROM ubuntu:22.04

# Avoid prompts from apt
ENV DEBIAN_FRONTEND=noninteractive

# Install dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    autoconf \
    libtool \
    pkg-config \
    git \
    cmake \
    libssl-dev \
    libsqlite3-dev \
    libgflags-dev \
    libsnappy-dev \
    zlib1g-dev \
    libbz2-dev \
    liblz4-dev \
    libzstd-dev \
    librocksdb-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Determine the install dir
ENV MY_INSTALL_DIR="/usr/local"
RUN mkdir -p $MY_INSTALL_DIR

# Clone the gRPC repo
RUN mkdir -p /root
WORKDIR /root
RUN git clone --recurse-submodules -b v1.54.0 --depth 1 --shallow-submodules https://github.com/grpc/grpc
WORKDIR /root/grpc

# Build and install gRPC
RUN mkdir -p build
WORKDIR /root/grpc/build
RUN cmake -DgRPC_INSTALL=ON -DgRPC_BUILD_TESTS=OFF -DCMAKE_INSTALL_PREFIX=$MY_INSTALL_DIR ..
RUN make -j$(nproc) && make install

# Set the working directory
RUN mkdir -p /working_dir
WORKDIR /working_dir