# Getting started with gh-ost development.

## Overview

Getting started with gh-ost development is simple!

- First clone the repository.
- From inside of the repository run `script/cibuild`
- This will bootstrap the environment if needed, format the code, build the code, and then run the unit test.

## CI build workflow

`script/cibuild` performs the following actions:

- It runs `script/bootstrap`
- `script/bootstrap` run `script/ensure-go-installed`
- `script/ensure-go-installed` installs go locally if (go is not installed) || (go is not version 1.7). It also will not install go if it is already installed locally.
- `script/build` builds the binary and places in in `bin/`

## Notes:

Currently, `script/ensure-go-installed` will install `go` for Mac OS X and Linux. We welcome PR's to add other platforms.

