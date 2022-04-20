drop table if exists gh_ost_test;
create table gh_ost_test (
  `id` decimal(32,0) NOT NULL DEFAULT '0',
  `name` varchar(16) NOT NULL DEFAULT '',
  primary key(id)
);
insert into gh_ost_test values(80700020220314193000, 'dms-zhanbing');

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
  set @nextKey := (select max(id)+1 from gh_ost_test);
  set @updateKey := (select id from gh_ost_test order by rand() limit 1);
  insert into gh_ost_test values (@nextKey, substring(md5(rand()), 1, 12));
  insert into gh_ost_test values (@nextKey+1, substring(md5(rand()), 1, 12));
  update gh_ost_test set name=substring(md5(rand()), 1, 12) order by id desc limit 1;
  delete from gh_ost_test where id=@updateKey;
end ;;
