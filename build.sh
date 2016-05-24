#!/bin/bash
#
#
RELEASE_VERSION="0.7.17"

buildpath=/tmp/gh-ost
target=gh-ost
timestamp=$(date "+%Y%m%d%H%M%S")
mkdir -p ${buildpath}
ldflags="-X main.AppVersion=${RELEASE_VERSION}"
gobuild="go build -ldflags \"$ldflags\" -o $buildpath/$target go/cmd/gh-ost/main.go"

echo "Building OS/X binary"
echo "GO15VENDOREXPERIMENT=1 GOOS=darwin GOARCH=amd64 $gobuild" | bash
(cd $buildpath && tar cfz ./gh-ost-binary-osx-${timestamp}.tar.gz $target)

echo "Building linux binary"
echo "GO15VENDOREXPERIMENT=1 GOOS=linux GOARCH=amd64 $gobuild" | bash
(cd $buildpath && tar cfz ./gh-ost-binary-linux-${timestamp}.tar.gz $target)

echo "Binaries found in:"
ls -1 $buildpath/gh-ost-binary*${timestamp}.tar.gz
