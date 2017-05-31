#!/bin/bash

source travis/00-_common.sh

set -xe

rm -rf $GOPATH
mkdir -p $(dirname $GHOST_SRC)
ln -s "$PWD" $GHOST_SRC

mkdir -p $BINDIR
version=$(git rev-parse HEAD)
describe=$(git describe --tags --always --dirty)

cd $GHOST_SRC

go build \
  -o $GHOST_BIN \
  -ldflags "-X main.AppVersion=${version} -X main.BuildDescribe=${describe}" \
  ./go/cmd/gh-ost/main.go
