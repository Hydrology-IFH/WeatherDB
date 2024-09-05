#!/usr/bin/env bash

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

cd $SCRIPT_DIR/..
docker compose -f ./docker/docker-compose.yaml -f ./docker/docker-compose_test.yaml up --build --watch