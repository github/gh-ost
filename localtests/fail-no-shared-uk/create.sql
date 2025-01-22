drop table if exists gh_ost_test;
create table gh_ost_test (
  id int not null,
  i int not null,
  ts timestamp,
  primary key(id)
);

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
  insert into gh_ost_test values (null, 11, now());
  insert into gh_ost_test values (null, 13, now());
  insert into gh_ost_test values (null, 17, now());
end ;;
