drop table if exists gh_ost_test;
create table gh_ost_test (
  id int auto_increment,
  name mediumtext not null,
  primary key (id)
) auto_increment=1;

insert into gh_ost_test (name)
select repeat('a', 1500)
from information_schema.columns
cross join information_schema.tables
limit 1000;
