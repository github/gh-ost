drop table if exists gh_ost_test_log;
drop table if exists gh_ost_test_stats;
drop table if exists gh_ost_test;
create table gh_ost_test (
  id int auto_increment,
  i int not null,
  color varchar(32),
  status enum('active', 'archived', 'deleted') default 'active',
  last_update timestamp default current_timestamp on update current_timestamp,
  primary key(id)
) auto_increment=1;

create table gh_ost_test_log (
  id int auto_increment,
  test_id int,
  action varchar(16),
  old_status varchar(16),
  new_status varchar(16),
  ts timestamp default current_timestamp,
  primary key(id)
);

create table gh_ost_test_stats (
  color varchar(32),
  status varchar(16),
  count int,
  primary key(color, status)
);

-- Create a complex trigger with multiple statements and conditional logic
drop trigger if exists gh_ost_test_au;
delimiter ;;
create trigger gh_ost_test_au after update on gh_ost_test
for each row
begin
  -- Log the change
  insert into gh_ost_test_log (test_id, action, old_status, new_status) 
  values (NEW.id, 'UPDATE', OLD.status, NEW.status);
  
  -- Update statistics based on old values
  if OLD.status != NEW.status then
    -- Decrement old status count
    insert into gh_ost_test_stats (color, status, count)
    values (OLD.color, OLD.status, -1)
    on duplicate key update count = count - 1;
    
    -- Increment new status count
    insert into gh_ost_test_stats (color, status, count)
    values (NEW.color, NEW.status, 1)
    on duplicate key update count = count + 1;
  end if;
  
  -- If color changed but status remained the same, update stats
  if OLD.color != NEW.color and OLD.status = NEW.status then
    -- Decrement old color count
    insert into gh_ost_test_stats (color, status, count)
    values (OLD.color, OLD.status, -1)
    on duplicate key update count = count - 1;
    
    -- Increment new color count
    insert into gh_ost_test_stats (color, status, count)
    values (NEW.color, NEW.status, 1)
    on duplicate key update count = count + 1;
  end if;
end ;;
delimiter ;

-- Create an insert trigger to initialize stats
drop trigger if exists gh_ost_test_ai;
delimiter ;;
create trigger gh_ost_test_ai after insert on gh_ost_test
for each row
begin
  -- Log the insertion
  insert into gh_ost_test_log (test_id, action, new_status) 
  values (NEW.id, 'INSERT', NEW.status);
  
  -- Update statistics
  insert into gh_ost_test_stats (color, status, count)
  values (NEW.color, NEW.status, 1)
  on duplicate key update count = count + 1;
end ;;
delimiter ;

-- Create a delete trigger to update stats
drop trigger if exists gh_ost_test_ad;
delimiter ;;
create trigger gh_ost_test_ad after delete on gh_ost_test
for each row
begin
  -- Log the deletion
  insert into gh_ost_test_log (test_id, action, old_status) 
  values (OLD.id, 'DELETE', OLD.status);
  
  -- Update statistics
  insert into gh_ost_test_stats (color, status, count)
  values (OLD.color, OLD.status, -1)
  on duplicate key update count = count + 1;
end ;;
delimiter ;

-- Insert initial data
insert into gh_ost_test values (null, 11, 'red', 'active', null);
insert into gh_ost_test values (null, 13, 'green', 'active', null);
insert into gh_ost_test values (null, 17, 'blue', 'active', null);

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
  insert into gh_ost_test values (null, 11, 'red', 'active', null);
  insert into gh_ost_test values (null, 13, 'green', 'active', null);
  update gh_ost_test set status='archived' where color='red' and status='active' limit 1;
  update gh_ost_test set color='purple' where color='blue' and status='active' limit 1;
  delete from gh_ost_test where status='archived' limit 1;
end ;;
delimiter ; 