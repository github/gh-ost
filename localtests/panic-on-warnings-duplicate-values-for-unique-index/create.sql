drop table if exists gh_ost_test;
create table gh_ost_test (
  id int auto_increment,
  name varchar(255) not null,
  primary key (id)
) auto_increment=1;

insert into gh_ost_test (`name`) values ('John');
insert into gh_ost_test (`name`) values ('John');

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
  insert ignore into gh_ost_test values ('John ' || last_insert_id());
  insert ignore into gh_ost_test values ('Adam ' || last_insert_id());
end ;;
