#!/bin/bash
#
#

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

  if ! go version | egrep -q 'go(1\.1[456])' ; then
    echo "go version must be 1.14 or above"
    exit 1
  fi

  echo "Building ${osname} binary"
  export GOOS
  export GOARCH
  go build -ldflags "$ldflags" -o $buildpath/$target go/cmd/gh-ost/main.go

  if [ $? -ne 0 ]; then
      echo "Build failed for ${osname}"
      exit 1
  fi

  (cd $buildpath && tar cfz ./gh-ost-binary-${osshort}-${timestamp}.tar.gz $target)

  if [ "$GOOS" == "linux" ] ; then
    echo "Creating Distro full packages"
    builddir=$(setuptree)
    cp $buildpath/$target $builddir/gh-ost/usr/bin
    cd $buildpath
    fpm -v "${RELEASE_VERSION}" --epoch 1 -f -s dir -n gh-ost -m 'shlomi-noach <shlomi-noach+gh-ost-deb@github.com>' --description "GitHub's Online Schema Migrations for MySQL " --url "https://github.com/github/gh-ost" --vendor "GitHub" --license "Apache 2.0" -C $builddir/gh-ost --prefix=/ -t rpm .
    fpm -v "${RELEASE_VERSION}" --epoch 1 -f -s dir -n gh-ost -m 'shlomi-noach <shlomi-noach+gh-ost-deb@github.com>' --description "GitHub's Online Schema Migrations for MySQL " --url "https://github.com/github/gh-ost" --vendor "GitHub" --license "Apache 2.0" -C $builddir/gh-ost --prefix=/ -t deb --deb-no-default-config-files .
  fi
}

main() {
  if [ -z "${RELEASE_VERSION}" ] ; then
    RELEASE_VERSION=$(git describe --abbrev=0 --tags | tr -d 'v')
  fi
  if [ -z "${RELEASE_VERSION}" ] ; then
    RELEASE_VERSION=$(cat RELEASE_VERSION)
  fi


  buildpath=/tmp/gh-ost-release
  target=gh-ost
  timestamp=$(date "+%Y%m%d%H%M%S")
  ldflags="-X main.AppVersion=${RELEASE_VERSION}"

  mkdir -p ${buildpath}
  rm -rf ${buildpath:?}/*
  build GNU/Linux linux linux amd64
  # build macOS osx darwin amd64

  echo "Binaries found in:"
  find $buildpath/gh-ost* -type f -maxdepth 1
}

main "$@"
