drop table if exists gh_ost_test;
create table gh_ost_test (
  id int auto_increment,
  t varchar(128) charset latin1 collate latin1_swedish_ci,
  primary key(id)
) auto_increment=1 charset utf8;

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
  set names latin1;
  insert into gh_ost_test values (null, 'א') ;
end ;;
