#!/bin/bash
#
#

buildpath=/tmp/gh-osc
target=gh-osc
timestamp=$(date "+%Y%m%d%H%M%S")
mkdir -p ${buildpath}
gobuild="go build -o $buildpath/$target go/cmd/gh-osc/main.go"

echo "Building OS/X binary"
echo "GO15VENDOREXPERIMENT=1 GOOS=darwin GOARCH=amd64 $gobuild" | bash
(cd $buildpath && tar cfz ./gh-osc-binary-osx-${timestamp}.tar.gz $target)

echo "Building linux binary"
echo "GO15VENDOREXPERIMENT=1 GOOS=linux GOARCH=amd64 $gobuild" | bash
(cd $buildpath && tar cfz ./gh-osc-binary-linux-${timestamp}.tar.gz $target)

echo "Binaries found in:"
ls -1 $buildpath/gh-osc-binary*${timestamp}.tar.gz
