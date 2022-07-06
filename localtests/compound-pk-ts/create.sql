drop table if exists gh_ost_test;
create table gh_ost_test (
  id int auto_increment,
  i int not null,
  ts0 timestamp(6) default current_timestamp(6),
  updated tinyint unsigned default 0,
  primary key(id, ts0)
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
  insert into gh_ost_test values (null, 11, sysdate(6), 0);
  update gh_ost_test set updated = 1 where i = 11 order by id desc limit 1;

  insert into gh_ost_test values (null, 13,  sysdate(6), 0);
  update gh_ost_test set updated = 1 where i = 13 order by id desc limit 1;

  insert into gh_ost_test values (null, 17, sysdate(6), 0);
  update gh_ost_test set updated = 1 where i = 17 order by id desc limit 1;

  insert into gh_ost_test values (null, 19, sysdate(6), 0);
  update gh_ost_test set updated = 1 where i = 19 order by id desc limit 1;

  insert into gh_ost_test values (null, 23, sysdate(6), 0);
  update gh_ost_test set updated = 1 where i = 23 order by id desc limit 1;

  insert into gh_ost_test values (null, 29, sysdate(6), 0);
  insert into gh_ost_test values (null, 31, sysdate(6), 0);
  insert into gh_ost_test values (null, 37, sysdate(6), 0);
  insert into gh_ost_test values (null, 41, sysdate(6), 0);
  delete from gh_ost_test where i = 31 order by id desc limit 1;
end ;;
