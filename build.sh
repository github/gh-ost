#!/bin/bash
#
#

RELEASE_VERSION="1.0.24"

function build {
    osname=$1
    osshort=$2
    GOOS=$3
    GOARCH=$4

    echo "Building ${osname} binary"
    export GOOS
    export GOARCH
    go build -ldflags "$ldflags" -o $buildpath/$target go/cmd/gh-ost/main.go

    if [ $? -ne 0 ]; then
        echo "Build failed for ${osname}"
        exit 1
    fi

    (cd $buildpath && tar cfz ./gh-ost-binary-${osshort}-${timestamp}.tar.gz $target)
}

buildpath=/tmp/gh-ost
target=gh-ost
timestamp=$(date "+%Y%m%d%H%M%S")
ldflags="-X main.AppVersion=${RELEASE_VERSION}"
export GO15VENDOREXPERIMENT=1

mkdir -p ${buildpath}
build macOS osx darwin amd64
build GNU/Linux linux linux amd64

echo "Binaries found in:"
ls -1 $buildpath/gh-ost-binary*${timestamp}.tar.gz
