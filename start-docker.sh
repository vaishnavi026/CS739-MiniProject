#! /bin/bash

export DOCKER_DEFAULT_PLATFORM=linux/amd64
export CURRENT_DIR=`pwd`
docker build -t g3:latest docker/.
docker kill g3
docker rm g3
docker run -d -v $CURRENT_DIR:/working_dir/ --name=g3 g3:latest sleep infinity
docker exec -it g3 bash
export PATH="$MY_INSTALL_DIR/bin:$PATH"