drop event if exists gh_ost_test;

drop table if exists gh_ost_test;
create table gh_ost_test (
  id int auto_increment,
  i int not null,
  primary key(id)
) auto_increment=1;

insert into gh_ost_test values (NULL, 11);
insert into gh_ost_test values (NULL, 13);
insert into gh_ost_test values (NULL, 17);
insert into gh_ost_test values (NULL, 23);
