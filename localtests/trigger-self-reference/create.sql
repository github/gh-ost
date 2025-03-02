drop table if exists gh_ost_test;
create table gh_ost_test (
  id int auto_increment,
  i int not null,
  color varchar(32),
  last_modified timestamp default current_timestamp,
  modified_count int default 0,
  primary key(id)
) auto_increment=1;

-- Create a trigger that modifies the same table
drop trigger if exists gh_ost_test_au;
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
  insert into gh_ost_test values (null, 11, 'red', null, 0);
  insert into gh_ost_test values (null, 13, 'green', null, 0);
  update gh_ost_test set color='yellow' where color='red' limit 1;
end ;;
delimiter ; 