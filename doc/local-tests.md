# Local tests

`gh-ost` is continuously tested in production via `--test-on-replica alter='engine=innodb'`. These tests check the GitHub workload and usage, but not necessarily the general case.

Local tests are an additional layer of tests used for continuous integration tests and local development.

Local tests test explicit use cases, such as column renames, mix of time zones, special types and alters. Traits of a single test:

- Composed of a single table.
- A single alter.
- By default the alter is `engine=innodb`, but this can be overridden per-test
- Scheduled DML operations, executed via `event_scheduler`.
- `gh-ost` is set to execute and throttle for `5` seconds, at which time all tested DMLs are expected to operate.
- The test requires a replication topology and utilizes `--test-on-replica`
- The test checksums the two tables (original and _ghost_) and expects identical checksum
- By default the test selects all (`*`) columns, but this can be overridden per-test

Tests are found under [localtests](https://github.com/github/gh-ost/tree/master/localtests). A single test is a subdirectory and tests are iterated alphabetically.

New data-integrity, synchronization issues or otherwise concerns are expected to be tested by new test cases.

## Run with docker compose

Local tests can be run locally with docker compose using the helper script [script/docker-gh-ost-replica-tests](https://github.com/github/gh-ost/tree/master/script/docker-gh-ost-replica-tests). 

Example usage:
```shell
# create primary-replica containers with specified mysql image
TEST_MYSQL_IMAGE="mysql-server:8.0.16" ./script/docker-gh-ost-replica-tests up

# run all tests
./script/docker-gh-ost-replica-tests run

# cleanup containers
./script/docker-gh-ost-replica-tests down
```

Pass the `-t` flag to run the tests with a toxiproxy between gh-ost and the MySQL replica. This simulates network conditions where MySQL connections are closed unexpectedly.

```shell
# run tests with toxiproxy
./script/docker-gh-ost-replica-tests up -t
./script/docker-gh-ost-replica-tests run -t
```
