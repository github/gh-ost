name: CI

on: [pull_request]

jobs:
  build:

    runs-on: ubuntu-latest

    steps:
    - uses: actions/checkout@v2

    - name: Set up Go 1.15
      uses: actions/setup-go@v1
      with:
        go-version: 1.15

    - name: Build
      run: script/cibuild

    - name: Upload gh-ost binary artifact
      uses: actions/upload-artifact@v1
      with:
        name: gh-ost
        path: bin/gh-ost
