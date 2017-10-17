drop table if exists gh_ost_test;
create table gh_ost_test (
  i int not null,
  ts timestamp default current_timestamp,
  dt datetime,
  key i_idx(i)
) auto_increment=1;

drop event if exists gh_ost_test;
