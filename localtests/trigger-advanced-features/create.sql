-- Drop riggers to ensure a clean slate
drop trigger if exists gh_ost_test_bi;
drop trigger if exists gh_ost_test_ai;
drop trigger if exists gh_ost_test_bu;
drop trigger if exists gh_ost_test_au;
drop trigger if exists gh_ost_test_bd;
drop trigger if exists gh_ost_test_ad;

-- Drop tables
drop table if exists gh_ost_test_log;
drop table if exists gh_ost_test_stats;
drop table if exists gh_ost_test;

-- Create main table
create table gh_ost_test (
  id int auto_increment,
  i int not null,
  color varchar(32),
  ts timestamp default current_timestamp,
  modified_count int default 0,
  primary key(id)
) auto_increment=1;

-- Create log table for trigger operations
create table gh_ost_test_log (
  id int auto_increment,
  test_id int,
  trigger_name varchar(128),
  action varchar(16),
  ts timestamp default current_timestamp,
  primary key(id)
);

-- Create stats table for complex test
create table gh_ost_test_stats (
  id int auto_increment,
  event_type varchar(32),
  i_sum int not null default 0,
  count_events int not null default 0,
  primary key(id),
  unique key event_type_uidx(event_type)
);

-- Create all types of triggers (BEFORE/AFTER with INSERT/UPDATE/DELETE)
-- Multiple trigger types from trigger-multiple test

-- BEFORE INSERT
delimiter ;;
create trigger gh_ost_test_bi before insert on gh_ost_test
for each row
begin
  insert into gh_ost_test_log (test_id, trigger_name, action) values (NEW.id, 'gh_ost_test_bi', 'BEFORE_INSERT');
end ;;
delimiter ;

-- AFTER INSERT
delimiter ;;
create trigger gh_ost_test_ai after insert on gh_ost_test
for each row
begin
  insert into gh_ost_test_log (test_id, trigger_name, action) values (NEW.id, 'gh_ost_test_ai', 'AFTER_INSERT');
  
  -- Complex logic from trigger-complex
  insert into gh_ost_test_stats (event_type, i_sum, count_events) 
  values ('insert', NEW.i, 1) 
  on duplicate key update i_sum=i_sum+NEW.i, count_events=count_events+1;
end ;;
delimiter ;

-- BEFORE UPDATE
delimiter ;;
create trigger gh_ost_test_bu before update on gh_ost_test
for each row
begin
  insert into gh_ost_test_log (test_id, trigger_name, action) values (NEW.id, 'gh_ost_test_bu', 'BEFORE_UPDATE');
end ;;
delimiter ;

-- AFTER UPDATE
delimiter ;;
create trigger gh_ost_test_au after update on gh_ost_test
for each row
begin
  insert into gh_ost_test_log (test_id, trigger_name, action) values (NEW.id, 'gh_ost_test_au', 'AFTER_UPDATE');
  
  -- Complex logic from trigger-complex
  insert into gh_ost_test_stats (event_type, i_sum, count_events) 
  values ('update', NEW.i-OLD.i, 1) 
  on duplicate key update i_sum=i_sum+(NEW.i-OLD.i), count_events=count_events+1;

  if NEW.color != OLD.color then
    update gh_ost_test set modified_count = modified_count + 1 where id = NEW.id;
  end if;
end ;;
delimiter ;

-- BEFORE DELETE
delimiter ;;
create trigger gh_ost_test_bd before delete on gh_ost_test
for each row
begin
  insert into gh_ost_test_log (test_id, trigger_name, action) values (OLD.id, 'gh_ost_test_bd', 'BEFORE_DELETE');
end ;;
delimiter ;

-- AFTER DELETE
delimiter ;;
create trigger gh_ost_test_ad after delete on gh_ost_test
for each row
begin
  insert into gh_ost_test_log (test_id, trigger_name, action) values (OLD.id, 'gh_ost_test_ad', 'AFTER_DELETE');
  
  -- Complex logic from trigger-complex
  insert into gh_ost_test_stats (event_type, i_sum, count_events) 
  values ('delete', -OLD.i, 1) 
  on duplicate key update i_sum=i_sum-OLD.i, count_events=count_events+1;
end ;;
delimiter ;

-- Insert initial data
insert into gh_ost_test values (null, 11, 'red', null, 0);
insert into gh_ost_test values (null, 13, 'green', null, 0);
insert into gh_ost_test values (null, 17, 'blue', null, 0);

-- Create event to test all trigger types with complex operations
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
  -- Test INSERT triggers
  insert into gh_ost_test values (null, 23, 'red', null, 0);
  
  -- Test UPDATE triggers with numeric change (for stats)
  update gh_ost_test set i=i+1 where id=1;
  
  -- Test UPDATE triggers with color change
  update gh_ost_test set color='yellow' where color='red' limit 1;
  
  -- Test DELETE triggers
  delete from gh_ost_test where id=2;
end ;;
delimiter ; 