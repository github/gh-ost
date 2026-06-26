#!/bin/bash

RELEASE_VERSION=
buildpath=

# Create a scratch tree to stage the package filesystem for fpm. It lives in the
# system temp dir, not $buildpath, so it never lands among the release artifacts.
function setuptree() {
  b=$( mktemp -d ) || return 1
  mkdir -p $b/gh-ost
  mkdir -p $b/gh-ost/usr/bin
  echo $b
}

function build {
  GOOS=$1
  GOARCH=$2

  if ! go version | egrep -q 'go1\.(1[5-9]|[2-9][0-9]{1})' ; then
    echo "go version must be 1.15 or above"
    exit 1
  fi

  echo "Building ${GOOS}-${GOARCH} binary"
  export GOOS
  export GOARCH
  CGO_ENABLED="${CGO_ENABLED:-0}" go build -ldflags "$ldflags" -o "$buildpath/$target" go/cmd/gh-ost/main.go

  if [ $? -ne 0 ]; then
      echo "Build failed for ${GOOS} ${GOARCH}."
      exit 1
  fi

  (cd $buildpath && tar cfz ./gh-ost-${GOOS}-${GOARCH}.tar.gz $target)

  # build RPM and deb packages for Linux
  if [ "$GOOS" == "linux" ] ; then
    echo "Creating Distro full packages"
    case "$GOARCH" in
      amd64) rpm_arch=x86_64  ; deb_arch=amd64 ;;
      arm64) rpm_arch=aarch64 ; deb_arch=arm64 ;;
      *)     rpm_arch=$GOARCH ; deb_arch=$GOARCH ;;
    esac
    builddir=$(setuptree)
    cp $buildpath/$target $builddir/gh-ost/usr/bin
    cd $buildpath

    fpm_opts=(
      -v "${RELEASE_VERSION}" --epoch 1 -f -s dir -n gh-ost
      -m 'GitHub' --description "GitHub's Online Schema Migrations for MySQL "
      --url "https://github.com/github/gh-ost" --vendor "GitHub" --license "Apache 2.0"
      -C "$builddir/gh-ost" --prefix=/
    )
    fpm "${fpm_opts[@]}" -t rpm -a "${rpm_arch}" -p "gh-ost.${rpm_arch}.rpm" --rpm-rpmbuild-define "_build_id_links none" --rpm-os linux .
    fpm "${fpm_opts[@]}" -t deb -a "${deb_arch}" -p "gh-ost.${deb_arch}.deb" --deb-no-default-config-files .
    cd -
  fi

  # Drop the bare binary so only tarballs and packages are published as release assets
  rm -f "$buildpath/$target"
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
  ldflags="-X main.AppVersion=${RELEASE_VERSION} -X main.GitCommit=${GIT_COMMIT}"

  mkdir -p ${buildpath}
  rm -rf ${buildpath:?}/*
  build linux amd64
  build linux arm64
  build darwin amd64
  build darwin arm64

  # Generate a SHA256SUMS manifest with basenames only, so a downloaded set can be
  # verified in place with `sha256sum -c SHA256SUMS` (no absolute build paths leak in).
  # The name has no gh-ost prefix, so it self-excludes from the gh-ost* glob below.
  echo "Checksums:"
  ( cd "$buildpath" && shasum -a256 gh-ost* | tee SHA256SUMS )

  echo "Release assets:"
  ls -1 "$buildpath"

  echo "Build Success!"
}

main "$@"
