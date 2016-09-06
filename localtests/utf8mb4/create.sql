drop table if exists gh_ost_test;
create table gh_ost_test (
  id int auto_increment,
  t varchar(128) charset utf8mb4,
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
  insert into gh_ost_test values (null, md5(rand()));
  insert into gh_ost_test values (null, 'átesting');
  insert into gh_ost_test values (null, '🍻😀');
end ;;
