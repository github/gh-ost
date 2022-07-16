# Getting started with gh-ost development.

## Overview

Getting started with gh-ost development is simple!

- First obtain the repository with `git clone` or `go get`.
- From inside of the repository run `script/cibuild`.
- This will bootstrap the environment if needed, format the code, build the code, and then run the unit test.

## CI build workflow

`script/cibuild` performs the following actions will bootstrap the environment to build `gh-ost` correctly, build, perform syntax checks and run unit tests.

If additional steps are needed, please add them into this workflow so that the workflow remains simple.

## `golang-ci` linter

To enfore best-practices, Pull Requests are automatically linted by [`golang-ci`](https://golangci-lint.run/). The linter config is located at [`.golangci.yml`](https://github.com/github/gh-ost/blob/master/.golangci.yml) and the `golangci-lint` GitHub Action is located at [`.github/workflows/golangci-lint.yml`](https://github.com/github/gh-ost/blob/master/.github/workflows/golangci-lint.yml).

To run the `golang-ci` linters locally _(recommended before push)_, use `script/lint`.

## Notes:

Currently, `script/ensure-go-installed` will install `go` for Mac OS X and Linux. We welcome PR's to add other platforms.
