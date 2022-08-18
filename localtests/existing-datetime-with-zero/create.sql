set session sql_mode='';
drop table if exists gh_ost_test;
create table gh_ost_test (
  id int unsigned auto_increment,
  i int not null,
  dt datetime not null default '1970-00-00 00:00:00',
  primary key(id)
) auto_increment=1;

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
  insert into gh_ost_test values (null, 7, '2010-10-20 10:20:30');
end ;;
