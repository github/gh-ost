## Getting started with gh-ost development.

Getting started with gh-ost development is simple!

- First clone the repository.
- From inside the repository directory, run ./script/bootstrap.
- From inside the repository directory, run ./test.sh to run the tests and validate your environment.

A couple of notes here:

- Any scripts you use for development should include `. ./scripts/env.sh` at the top. All environment variables, `$GOPATH`, `$GOROOT`, etc are set here so that all scripts can use them.
- `$GOCMD` is set to use `go 1.7` installed inside the repository. This is done so that all development is using the same version of go for easy troubleshooting.
