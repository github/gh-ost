-- Drop triggers to ensure a clean slate
drop trigger if exists gh_ost_test_ai;
drop trigger if exists gh_ost_test_au;

-- Drop tables
drop table if exists gh_ost_test_log;
drop table if exists gh_ost_test;

-- Create main table with fields needed for both simple and self-reference tests
create table gh_ost_test (
  id int auto_increment,
  i int not null,
  color varchar(32),
  last_modified timestamp default current_timestamp,
  modified_count int default 0,
  primary key(id)
) auto_increment=1;

-- Create log table for basic trigger activity
create table gh_ost_test_log (
  id int auto_increment,
  test_id int,
  action varchar(16),
  ts timestamp default current_timestamp,
  primary key(id)
);

-- Test 1: Create a simple AFTER INSERT trigger (from trigger-basic)
delimiter ;;
create trigger gh_ost_test_ai after insert on gh_ost_test
for each row
begin
  insert into gh_ost_test_log (test_id, action) values (NEW.id, 'INSERT');
end ;;
delimiter ;

-- Test 2: Create a self-referencing AFTER UPDATE trigger (from trigger-self-reference)
delimiter ;;
create trigger gh_ost_test_au after update on gh_ost_test
for each row
begin
  -- This is a self-referencing trigger that updates the same table
  -- It should be handled correctly by gh-ost
  if NEW.color != OLD.color then
    update gh_ost_test set modified_count = modified_count + 1 where id = NEW.id;
  end if;
end ;;
delimiter ;

-- Insert initial data
insert into gh_ost_test values (null, 11, 'red', null, 0);
insert into gh_ost_test values (null, 13, 'green', null, 0);
insert into gh_ost_test values (null, 17, 'blue', null, 0);

-- Create event to execute both INSERT and UPDATE operations to test both triggers
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
  -- Test INSERT trigger
  insert into gh_ost_test values (null, 11, 'red', null, 0);
  insert into gh_ost_test values (null, 13, 'green', null, 0);
  
  -- Test UPDATE/self-referencing trigger
  update gh_ost_test set color='yellow' where color='red' limit 1;
end ;;
delimiter ; 