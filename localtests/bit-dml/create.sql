drop table if exists gh_ost_test;
create table gh_ost_test (
  id int auto_increment,
  i int not null,
  is_good bit null default 0,
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
  insert into gh_ost_test values (null, 11, 0);
  insert into gh_ost_test values (null, 13, 1);
  insert into gh_ost_test values (null, 17, 1);

  update gh_ost_test set is_good=0 where i=13 order by id desc limit 1;
end ;;
