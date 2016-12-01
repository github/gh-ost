#!/bin/bash

set -e

. script/bootstrap

mkdir -p bin
bindir="$PWD"/bin

cd .gopath/src/github.com/github/gh-ost
go "$@"
