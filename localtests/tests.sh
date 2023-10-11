#!/bin/bash

set -e

LOCALTESTS_DIR="$(readlink -f $(dirname $0))"

if [ -z "$TEST_DOCKER_IMAGE" ]; then
  echo "TEST_DOCKER_IMAGE env var must be set"
  exit 1
fi

if [ -z "$TEST_STORAGE_ENGINE" ]; then
  TEST_STORAGE_ENGINE=innodb
fi  

# generate env file for containers
generate_test_env() {
  [ ! -z "$GITHUB_ACTION" ] && echo "::group::generate mysql env"

  (
    echo "GITHUB_ACTION=${GITHUB_ACTION}"
    echo 'MYSQL_ALLOW_EMPTY_PASSWORD=true'
    echo "TEST_STORAGE_ENGINE=${TEST_STORAGE_ENGINE}"
    [ "$TEST_STORAGE_ENGINE" == "rocksdb" ] && echo 'INIT_ROCKSDB=true'
  ) | tee $LOCALTESTS_DIR/tests.env
  echo "Wrote env file to $LOCALTESTS_DIR/tests.env"

  [ ! -z "$GITHUB_ACTION" ] && echo "::endgroup::"
  return 0
}

# prebuild the test docker image
prebuild_docker_image() {
  echo "::group::docker image build"
  docker-compose -f localtests/docker-compose.yml build
  echo "::endgroup::"
}

###

generate_test_env

# conditional pre-build
EXTRA_UP_FLAGS=
if [ -z "$GITHUB_ACTION" ]; then
  EXTRA_UP_FLAGS="--build"
else
  prebuild_docker_image
fi

# start test container and mysql primary/replica
# with docker-compose
docker-compose -f localtests/docker-compose.yml up \
  --abort-on-container-exit \
  --no-log-prefix \
  $EXTRA_UP_FLAGS \
  tests
