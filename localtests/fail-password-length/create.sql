drop table if exists gh_ost_test;
create table gh_ost_test (
  id int auto_increment,
  i int not null,
  ts timestamp,
  primary key(id)
) auto_increment=1;
