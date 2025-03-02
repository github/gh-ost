-- Drop both original and ghost triggers to ensure a clean slate
drop trigger if exists gh_ost_test_ai;
drop table if exists gh_ost_test_log;
drop table if exists gh_ost_test;
create table gh_ost_test (
  id int auto_increment,
  i int not null,
  color varchar(32),
  primary key(id)
) auto_increment=1;

create table gh_ost_test_log (
  id int auto_increment,
  test_id int,
  action varchar(16),
  ts timestamp default current_timestamp,
  primary key(id)
);

-- Create a simple AFTER INSERT trigger
delimiter ;;
create trigger gh_ost_test_ai after insert on gh_ost_test
for each row
begin
  insert into gh_ost_test_log (test_id, action) values (NEW.id, 'INSERT');
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
  insert into gh_ost_test values (null, 11, 'red');
  insert into gh_ost_test values (null, 13, 'green');
  insert into gh_ost_test values (null, 17, 'blue');
end ;;
delimiter ; 