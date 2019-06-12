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

delimiter ;;
create event gh_ost_test
  on schedule every 2 second
  starts current_timestamp
  ends current_timestamp + interval 60 second
  on completion not preserve
  enable
  do
begin
  insert into gh_ost_test values (null, 11, 'orange');
  insert into gh_ost_test values (null, 13, 'brown');
end ;;
