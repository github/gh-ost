#!/bin/bash

set -e

. ./script/common
buildpath=build
timestamp=$(date "+%Y%m%d%H%M%S")

function build() {
  goos=$1
  osshort=$goos

  envs="-e GOOS=$goos \
    -e GOARCH=amd64 \
    -e CGO_ENABLED=1 \
    -e GOCACHE=/go/src/${PACKAGE_PATH}/.cache \
    -e RELEASE_VERSION=${RELEASE_VERSION} \
    -e BIN_PATH=${buildpath}/bin"

  if [ "$goos" == "darwin" ]; then
    # specify cross compiler
    envs="$envs -e CC=x86_64h-apple-darwin14-cc"
    osshort="osx"
  fi

  echo "Building $goos binary in ${buildpath}/bin"

  docker run -u $(id -u):$(id -g) --rm $envs \
    -v $(pwd):/go/src/${PACKAGE_PATH} \
    -w /go/src/${PACKAGE_PATH} \
    dockercore/golang-cross:$GO_VERSION \
    bash -c 'go build -ldflags "-X main.AppVersion=${RELEASE_VERSION}" -o ${BIN_PATH}/gh-ost-$GOOS-$GOARCH go/cmd/gh-ost/main.go'

  (
    cp ${buildpath}/bin/gh-ost-$goos-amd64 $buildpath/dist/gh-ost \
    && cd $buildpath/dist \
    && tar cfz ./gh-ost-binary-${osshort}-${timestamp}.tar.gz gh-ost\
    && rm gh-ost
  )
}

function distro_packages() {
  echo "Creating Distro full packages"
  docker build -t gh-ost/fpm -f docker/fpm/Dockerfile docker/fpm

  fpm_cmd="docker run -u $(id -u):$(id -g) --rm \
    -e RELEASE_VERSION=${RELEASE_VERSION} \
    -e PACKAGE_PATH=${PACKAGE_PATH} \
    -e BIN_PATH=/go/src/${PACKAGE_PATH}/build/bin \
    -e OUTPUT_PATH=/go/src/${PACKAGE_PATH}/build/dist \
    -w /gh-ost \
    -v $(pwd):/go/src/${PACKAGE_PATH} \
    gh-ost/fpm"

  $fpm_cmd bash -c 'mkdir -p /gh-ost/usr/bin \
      && cp ${BIN_PATH}/gh-ost-linux-amd64 /gh-ost/usr/bin/gh-ost \
      && cd /gh-ost \
      && fpm -v "${RELEASE_VERSION}" --epoch 1 -f -s dir -n gh-ost -m "shlomi-noach <shlomi-noach+gh-ost-deb@github.com>" --description "GitHub'"'"'s Online Schema Migrations for MySQL " --url "https://${PACKAGE_PATH}" --vendor "GitHub" --license "Apache 2.0" -C /gh-ost --prefix=/ -t rpm . \
      && cp *.rpm ${OUTPUT_PATH}'

  $fpm_cmd bash -c 'mkdir -p /gh-ost/usr/bin \
      && cp ${BIN_PATH}/gh-ost-linux-amd64 /gh-ost/usr/bin/gh-ost \
      && cd /gh-ost \
      && fpm -v "${RELEASE_VERSION}" --epoch 1 -f -s dir -n gh-ost -m "shlomi-noach <shlomi-noach+gh-ost-deb@github.com>" --description "GitHub'"'"'s Online Schema Migrations for MySQL " --url "https://github.com/github/gh-ost" --vendor "GitHub" --license "Apache 2.0" -C /gh-ost --prefix=/ -t deb --deb-no-default-config-files . \
      && cp *.deb ${OUTPUT_PATH}'
}

mkdir -p ${buildpath}
rm -rf ${buildpath:?}/*
mkdir ${buildpath}/{bin,dist}

build linux
build darwin
distro_packages

echo "Binaries found in:"
ls -1 $buildpath/dist/gh-ost-binary*${timestamp}.tar.gz
