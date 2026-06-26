-- Atomic multi-table cutover test.
--
-- Two tables of identical shape with a correlation column `txn_id`. The test
-- workload (see test.sh) commits transactions that write the SAME txn_id into
-- BOTH tables. Because all migrated tables are renamed in a single atomic
-- `RENAME TABLE t1 TO ..., t2 TO ...` at cutover, every such transaction lands on
-- the target entirely or not at all -- so the target tables must hold exactly the
-- same set of txn_ids. A regression to per-table sequential RENAME would split a
-- boundary transaction and leave an orphan.

drop table if exists gh_ost_test;
create table gh_ost_test (
  id int(11) NOT NULL AUTO_INCREMENT,
  txn_id int(11) NOT NULL,
  payload varchar(32) NOT NULL,
  PRIMARY KEY (id),
  KEY txn_ix (txn_id)
);

insert into gh_ost_test (txn_id, payload) values
  (0, 'seed'), (0, 'seed'), (0, 'seed'), (0, 'seed'), (0, 'seed');

drop table if exists gh_ost_test_other;
create table gh_ost_test_other (
  id int(11) NOT NULL AUTO_INCREMENT,
  txn_id int(11) NOT NULL,
  payload varchar(32) NOT NULL,
  PRIMARY KEY (id),
  KEY txn_ix (txn_id)
);

insert into gh_ost_test_other (txn_id, payload) values
  (0, 'seed'), (0, 'seed'), (0, 'seed'), (0, 'seed'), (0, 'seed');
