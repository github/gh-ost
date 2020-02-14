set session time_zone='+00:00';

drop table if exists gh_ost_test;
create table gh_ost_test (
  id int auto_increment,
  create_time timestamp NULL DEFAULT '0000-00-00 00:00:00',
  update_time timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  counter int(10) unsigned DEFAULT NULL,
  primary key(id)
) auto_increment=1;

set session time_zone='+00:00';
insert into gh_ost_test values (1, '0000-00-00 00:00:00', now(), 0);

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
  set session time_zone='+00:00';
  update gh_ost_test set counter = counter + 1 where id = 1;
end ;;
