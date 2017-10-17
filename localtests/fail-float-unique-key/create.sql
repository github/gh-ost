drop table if exists gh_ost_test;
create table gh_ost_test (
  f float,
  i int not null,
  ts timestamp default current_timestamp,
  dt datetime,
  key i_idx(i),
  unique key f_uidx(f)
) auto_increment=1;

drop event if exists gh_ost_test;
