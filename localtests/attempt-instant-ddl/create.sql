drop table if exists gh_ost_test;
create table gh_ost_test (
  id int auto_increment,
  i int not null,
  color varchar(32),
  primary key(id)
) auto_increment=1;

drop event if exists gh_ost_test;

insert into gh_ost_test values (null, 11, 'red');
insert into gh_ost_test values (null, 13, 'green');
insert into gh_ost_test values (null, 17, 'blue');
