drop table if exists gh_ost_test;
create table gh_ost_test (
  id int auto_increment,
  email varchar(100) not null,
  primary key (id)
) auto_increment=1;

insert into gh_ost_test (email) values ('alice@example.com');
insert into gh_ost_test (email) values ('bob@example.com');
insert into gh_ost_test (email) values ('charlie@example.com');

drop event if exists gh_ost_test;
delimiter ;;
create event gh_ost_test
  on schedule every 1 second
  starts current_timestamp + interval 3 second
  ends current_timestamp + interval 60 second
  on completion not preserve
  enable
  do
begin
  -- This UPDATE modifies the primary key, so it will be converted to DELETE + INSERT
  -- The INSERT will attempt to insert email='alice@example.com' (duplicate)
  -- which violates the new unique index being added by the migration
  -- Delay ensures this fires during binlog apply phase, not bulk copy
  update gh_ost_test set id=10, email='alice@example.com' where id=2;
end ;;
