#!/bin/bash

set -e

LOCALTESTS_DIR="$(readlink -f $(dirname $0))"

if [ -z "$TEST_DOCKER_IMAGE" ]; then
  echo "TEST_DOCKER_IMAGE env var must be set"
  exit 1
fi

# generate mysql.env file for containers
[ ! -z "$GITHUB_ACTION" ] && echo "::group::generate mysql env"
(
  echo "GITHUB_ACTION=${GITHUB_ACTION}"
  echo 'MYSQL_ALLOW_EMPTY_PASSWORD=true'
  echo "TEST_STORAGE_ENGINE=${TEST_STORAGE_ENGINE}"
  [ "$TEST_STORAGE_ENGINE" == "rocksdb" ] && echo 'INIT_ROCKSDB=true'
) | tee $LOCALTESTS_DIR/mysql.env
echo "Wrote env file to $LOCALTESTS_DIR/mysql.env"
[ ! -z "$GITHUB_ACTION" ] && echo "::endgroup::"

# conditional pre-build
EXTRA_UP_FLAGS=
if [ -z "$GITHUB_ACTION" ]; then
  EXTRA_UP_FLAGS="--build"
else
  echo "::group::docker image build"
  docker-compose -f localtests/docker-compose.yml build
  echo "::endgroup::"
fi

# this will start the test container and the
# mysql primary/replica w/docker-compose
docker-compose -f localtests/docker-compose.yml up \
  --abort-on-container-exit \
  --no-log-prefix \
  $EXTRA_UP_FLAGS \
  tests
