#!/bin/bash

source travis/00-_common.sh

set -xe

cd $GHOST_SRC
go test ./go/...
