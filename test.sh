#!/bin/bash

retval=0

for testsuite in base mysql sql
do
  pushd go/${testsuite} > /dev/null;

  # TODO: remove GO111MODULE once gh-ost uses Go modules
  GO111MODULE=off go test $*;

  [ $? -ne 0 ] && retval=1
  popd > /dev/null;
done

exit $retval
