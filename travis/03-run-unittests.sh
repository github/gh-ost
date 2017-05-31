#!/bin/bash

set -xe

source travis/00-_common.sh

cd $GHOST_SRC
go test ./go/...
