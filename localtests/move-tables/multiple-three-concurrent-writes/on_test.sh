#!/bin/bash

# Three-table move with sustained DML on all three tables during the
# copy. insert-source-primary-loop auto-detects every seeded fixture
# (gh_ost_test, gh_ost_test_other, gh_ost_test_third) and writes to all of them,
# so each migrated table sees concurrent inserts while gh-ost copies and drains.
# The harness then validates per-table structure + content checksums (source
# `_<table>_del` vs target), which deterministically proves every concurrent
# write was captured on the target.
DATABASE=test script/move-tables/insert-source-primary-loop 100 0.01 100 &
sleep 5 && kill $!
