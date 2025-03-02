drop table if exists gh_ost_test_reference;
drop table if exists gh_ost_test;
create table gh_ost_test_reference (
  id int auto_increment,
  name varchar(32),
  primary key(id)
);

create table gh_ost_test (
  id int auto_increment,
  i int not null,
  color varchar(32),
  primary key(id)
) auto_increment=1;

-- Insert some reference data
insert into gh_ost_test_reference values (1, 'red');
insert into gh_ost_test_reference values (2, 'green');
insert into gh_ost_test_reference values (3, 'blue');

-- Create a trigger that references another table
drop trigger if exists gh_ost_test_bi;
delimiter ;;
create trigger gh_ost_test_bi before insert on gh_ost_test
for each row
begin
  -- This trigger references another table, which is not supported
  declare color_exists int;
  select count(*) into color_exists from gh_ost_test_reference where name = NEW.color;
  if color_exists = 0 then
    signal sqlstate '45000' set message_text = 'Invalid color';
  end if;
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