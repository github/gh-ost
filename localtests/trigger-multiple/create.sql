drop table if exists gh_ost_test_log;
drop table if exists gh_ost_test;
create table gh_ost_test (
  id int auto_increment,
  i int not null,
  color varchar(32),
  last_update timestamp default current_timestamp on update current_timestamp,
  primary key(id)
) auto_increment=1;

create table gh_ost_test_log (
  id int auto_increment,
  test_id int,
  action varchar(16),
  ts timestamp default current_timestamp,
  primary key(id)
);

-- Create multiple triggers for all DML operations
-- BEFORE INSERT trigger
drop trigger if exists gh_ost_test_bi;
delimiter ;;
create trigger gh_ost_test_bi before insert on gh_ost_test
for each row
begin
  if NEW.color = 'forbidden' then
    set NEW.color = 'allowed';
  end if;
end ;;
delimiter ;

-- AFTER INSERT trigger
drop trigger if exists gh_ost_test_ai;
delimiter ;;
create trigger gh_ost_test_ai after insert on gh_ost_test
for each row
begin
  insert into gh_ost_test_log (test_id, action) values (NEW.id, 'INSERT');
end ;;
delimiter ;

-- BEFORE UPDATE trigger
drop trigger if exists gh_ost_test_bu;
delimiter ;;
create trigger gh_ost_test_bu before update on gh_ost_test
for each row
begin
  if NEW.color = 'forbidden' then
    set NEW.color = 'allowed';
  end if;
end ;;
delimiter ;

-- AFTER UPDATE trigger
drop trigger if exists gh_ost_test_au;
delimiter ;;
create trigger gh_ost_test_au after update on gh_ost_test
for each row
begin
  insert into gh_ost_test_log (test_id, action) values (NEW.id, 'UPDATE');
end ;;
delimiter ;

-- BEFORE DELETE trigger
drop trigger if exists gh_ost_test_bd;
delimiter ;;
create trigger gh_ost_test_bd before delete on gh_ost_test
for each row
begin
  insert into gh_ost_test_log (test_id, action) values (OLD.id, 'PRE-DELETE');
end ;;
delimiter ;

-- AFTER DELETE trigger
drop trigger if exists gh_ost_test_ad;
delimiter ;;
create trigger gh_ost_test_ad after delete on gh_ost_test
for each row
begin
  insert into gh_ost_test_log (test_id, action) values (OLD.id, 'DELETE');
end ;;
delimiter ;

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
  insert into gh_ost_test values (null, 11, 'red', null);
  insert into gh_ost_test values (null, 13, 'forbidden', null);
  update gh_ost_test set color='yellow' where color='red' limit 1;
  delete from gh_ost_test where color='allowed' limit 1;
end ;;
delimiter ; 