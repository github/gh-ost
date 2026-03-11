drop table if exists gh_ost_test;
create table gh_ost_test (
  id int auto_increment,
  email varchar(100) not null,
  primary key (id)
) auto_increment=1;

insert into gh_ost_test (email) values ('alice@example.com');
insert into gh_ost_test (email) values ('bob@example.com');
insert into gh_ost_test (email) values ('charlie@example.com');

-- Add enough rows to give a window for the event to fire
-- With chunk-size=10, 100 rows = 10 chunks
insert into gh_ost_test (email)
select concat('user', @row := @row + 1, '@example.com')
from (select @row := 3) init,
     (select 0 union all select 1 union all select 2 union all select 3 union all select 4 union all select 5 union all select 6 union all select 7 union all select 8 union all select 9) t1,
     (select 0 union all select 1 union all select 2 union all select 3 union all select 4 union all select 5 union all select 6 union all select 7 union all select 8 union all select 9) t2
limit 100;

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
  -- Poll for row copy to complete by checking if last row has been copied
  -- Once row copy is done but before cutover, fire the UPDATE that creates a duplicate
  -- Check a flag table to ensure we only fire once
  if exists (select 1 from test._gh_ost_test_gho where id >= 100)
     and not exists (select 1 from information_schema.tables where table_schema='test' and table_name='_gh_ost_fired') then
    create table test._gh_ost_fired (id int);
    -- This UPDATE modifies the primary key, so it will be converted to DELETE + INSERT
    -- The INSERT will attempt to insert email='alice@example.com' (duplicate)
    -- which violates the new unique index being added by the migration
    update gh_ost_test set id=200, email='alice@example.com' where id=2;
  end if;
end ;;
