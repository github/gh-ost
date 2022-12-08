#!/bin/bash

set -e

export TEST_DOCKER_IMAGE=${1:-mysql:5.7}

LOCALTESTS_DIR="$(readlink -f $(dirname $0))"
cd $LOCALTESTS_DIR

# generate mysql.env file for containers
[ ! -z "$GITHUB_ACTION" ] && echo "::group::generate mysql env"
./mysql-env.sh
[ ! -z "$GITHUB_ACTION" ] && echo "::endgroup::"

# conditional pre-build
EXTRA_UP_FLAGS=
if [ -z "$GITHUB_ACTION" ]; then
  EXTRA_UP_FLAGS="--build"
else
  echo "::group::docker image build"
  docker-compose build
  echo "::endgroup::"
fi

# this will start the test container and the
# mysql primary/replica w/docker-compose
docker-compose up \
  --abort-on-container-exit \
  --no-log-prefix \
  $EXTRA_UP_FLAGS \
  tests
