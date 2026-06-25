drop table if exists gh_ost_test;
create table gh_ost_test (
  id int auto_increment,
  t varchar(128) charset utf8mb4,
  primary key(id)
) auto_increment=1;

-- Seed ~8k rows. Small enough that the resume copies quickly, while a high
-- --nice-ratio (see extra_args) stretches the first run's row-copy so it still
-- outlasts the first checkpoint (>=10s) and can be interrupted mid-copy.
-- Doubling INSERT ... SELECT is portable across MySQL 5.7/8.x and MariaDB.
insert into gh_ost_test (t) values (md5(rand())), (md5(rand())), (md5(rand())), (md5(rand()));
insert into gh_ost_test (t) select md5(rand()) from gh_ost_test;
insert into gh_ost_test (t) select md5(rand()) from gh_ost_test;
insert into gh_ost_test (t) select md5(rand()) from gh_ost_test;
insert into gh_ost_test (t) select md5(rand()) from gh_ost_test;
insert into gh_ost_test (t) select md5(rand()) from gh_ost_test;
insert into gh_ost_test (t) select md5(rand()) from gh_ost_test;
insert into gh_ost_test (t) select md5(rand()) from gh_ost_test;
insert into gh_ost_test (t) select md5(rand()) from gh_ost_test;
insert into gh_ost_test (t) select md5(rand()) from gh_ost_test;
insert into gh_ost_test (t) select md5(rand()) from gh_ost_test;
insert into gh_ost_test (t) select md5(rand()) from gh_ost_test;
