drop table if exists gh_ost_test;
create table gh_ost_test (
  id int auto_increment,
  email varchar(100) not null,
  primary key (id)
) auto_increment=1;

insert into gh_ost_test (email) values ('alice@example.com');
insert into gh_ost_test (email) values ('bob@example.com');
insert into gh_ost_test (email) values ('charlie@example.com');

-- Add many rows to extend row copy duration significantly
-- Need enough rows that copying takes multiple seconds to give event time to detect
-- With chunk-size=10, 1000 rows = 100 chunks which should take several seconds
insert into gh_ost_test (email)
select concat('user', @row := @row + 1, '@example.com')
from (select @row := 3) init,
     (select 0 union all select 1 union all select 2 union all select 3 union all select 4 union all select 5 union all select 6 union all select 7 union all select 8 union all select 9) t1,
     (select 0 union all select 1 union all select 2 union all select 3 union all select 4 union all select 5 union all select 6 union all select 7 union all select 8 union all select 9) t2,
     (select 0 union all select 1 union all select 2 union all select 3 union all select 4 union all select 5 union all select 6 union all select 7 union all select 8 union all select 9) t3
limit 1000;

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
  -- Poll for row copy to start by checking if alice has been copied to ghost table
  -- Once row copy has started, fire the UPDATE that creates a duplicate
  if exists (select 1 from test._gh_ost_test_gho where email = 'alice@example.com') then
    -- This UPDATE modifies the primary key, so it will be converted to DELETE + INSERT
    -- The INSERT will attempt to insert email='alice@example.com' (duplicate)
    -- which violates the new unique index being added by the migration
    update gh_ost_test set id=10, email='alice@example.com' where id=2;
  end if;
end ;;
