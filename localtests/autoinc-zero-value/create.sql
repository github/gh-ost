drop table if exists gh_ost_test;
create table gh_ost_test (
  id int auto_increment,
  i int not null,
  primary key(id)
) auto_increment=1;

set session sql_mode='NO_AUTO_VALUE_ON_ZERO';
insert into gh_ost_test values (0, 23);
