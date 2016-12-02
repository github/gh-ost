drop table if exists gh_ost_test;
create table gh_ost_test (
  id int auto_increment,
  c1 int not null,
  primary key (id)
) auto_increment=1;

drop event if exists gh_ost_test;
