drop table if exists gh_ost_test;
create table gh_ost_test (
  id int auto_increment,
  t text charset latin1 collate latin1_swedish_ci,
  primary key(id)
) auto_increment=1 charset latin1 collate latin1_swedish_ci;

insert into gh_ost_test values (null, char(189));

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
  insert into gh_ost_test values (null, char(189));
  update gh_ost_test set t=char(190) order by id desc limit 1;
  delete from gh_ost_test where t=char(190);
end ;;
