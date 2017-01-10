drop table if exists gh_ost_test;
create table gh_ost_test (
  id bigint,
  i int not null,
  ts timestamp(6),
  primary key(id),
  unique key its_uidx(i, ts)
) ;

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
  insert into gh_ost_test values ((unix_timestamp() << 2) + 0, 11, now(6));
  insert into gh_ost_test values ((unix_timestamp() << 2) + 1, 13, now(6));
  insert into gh_ost_test values ((unix_timestamp() << 2) + 2, 17, now(6));
  insert into gh_ost_test values ((unix_timestamp() << 2) + 3, 19, now(6));
end ;;
