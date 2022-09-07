drop table if exists gh_ost_test;
create table gh_ost_test (
  id int auto_increment,
  i int not null,
  result enum('Pass', 'Fail') not null,
  primary key(id)
) auto_increment=1 default charset=latin1;

insert into gh_ost_test values (1, 7, 'Pass');
insert into gh_ost_test values (2, 8, 'Fail');
insert into gh_ost_test values (3, 19, 'Pass');

drop event if exists gh_ost_test;
delimiter ;;
create event gh_ost_test
  on schedule every 1 second
  starts current_timestamp
  ends current_timestamp + interval 60 second
  on completion not preserve
  enable
  do
begin
  insert into gh_ost_test (i, result) values(12345, 0);
  insert into gh_ost_test (i, result) values(12345, 1);
end ;;
