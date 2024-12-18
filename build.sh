#!/bin/bash

RELEASE_VERSION=
buildpath=

function setuptree() {
  b=$( mktemp -d $buildpath/gh-ostXXXXXX ) || return 1
  mkdir -p $b/gh-ost
  mkdir -p $b/gh-ost/usr/bin
  echo $b
}

function build {
  osname=$1
  osshort=$2
  GOOS=$3
  GOARCH=$4

  if ! go version | egrep -q 'go1\.(1[5-9]|[2-9][0-9]{1})' ; then
    echo "go version must be 1.15 or above"
    exit 1
  fi

  echo "Building ${osname}-${GOARCH} binary"
  export GOOS
  export GOARCH
  go build -ldflags "$ldflags" -o $buildpath/$target go/cmd/gh-ost/main.go

  if [ $? -ne 0 ]; then
      echo "Build failed for ${osname} ${GOARCH}."
      exit 1
  fi

  (cd $buildpath && tar cfz ./gh-ost-binary-${osshort}-${GOARCH}-${timestamp}.tar.gz $target)

  # build RPM and deb for Linux, x86-64 only
  if [ "$GOOS" == "linux" ] && [ "$GOARCH" == "amd64" ] ; then
    echo "Creating Distro full packages"
    builddir=$(setuptree)
    cp $buildpath/$target $builddir/gh-ost/usr/bin
    cd $buildpath
    fpm -v "${RELEASE_VERSION}" --epoch 1 -f -s dir -n gh-ost -m 'GitHub' --description "GitHub's Online Schema Migrations for MySQL " --url "https://github.com/github/gh-ost" --vendor "GitHub" --license "Apache 2.0" -C $builddir/gh-ost --prefix=/ -t rpm --rpm-rpmbuild-define "_build_id_links none" --rpm-os linux .
    fpm -v "${RELEASE_VERSION}" --epoch 1 -f -s dir -n gh-ost -m 'GitHub' --description "GitHub's Online Schema Migrations for MySQL " --url "https://github.com/github/gh-ost" --vendor "GitHub" --license "Apache 2.0" -C $builddir/gh-ost --prefix=/ -t deb --deb-no-default-config-files .
    cd -
  fi
}

main() {
  if [ -z "${RELEASE_VERSION}" ] ; then
    RELEASE_VERSION=$(git describe --abbrev=0 --tags | tr -d 'v')
  fi
  if [ -z "${RELEASE_VERSION}" ] ; then
    echo "RELEASE_VERSION must be set"
    exit 1
  fi

  if [ -z "${GIT_COMMIT}" ]; then
    GIT_COMMIT=$(git rev-parse HEAD)
  fi

  buildpath=/tmp/gh-ost-release
  target=gh-ost
  timestamp=$(date "+%Y%m%d%H%M%S")
  ldflags="-X main.AppVersion=${RELEASE_VERSION} -X main.GitCommit=${GIT_COMMIT}"

  mkdir -p ${buildpath}
  rm -rf ${buildpath:?}/*
  build GNU/Linux linux linux amd64
  build GNU/Linux linux linux arm64
  build macOS osx darwin amd64
  build macOS osx darwin arm64

  bin_files=$(find $buildpath/gh-ost* -type f -maxdepth 1)
  echo "Binaries found in:"
  echo "$bin_files"

  echo "Checksums:"
  (shasum -a256 $bin_files 2>/dev/null)

  echo "Build Success!"
}

main "$@"
