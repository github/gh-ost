drop event if exists gh_ost_test;
drop table if exists gh_ost_test;
create table gh_ost_test (
  id int auto_increment,
  code int not null,
  name varchar(64) not null default '',
  primary key(id),
  unique key uk_code (code)
) auto_increment=1;

insert into gh_ost_test (code, name) values (100, 'a'), (200, 'b'), (300, 'c'), (400, 'd'), (500, 'e');
insert into gh_ost_test (code, name) values (600, 'f'), (700, 'g'), (800, 'h'), (900, 'i'), (1000, 'j');

drop event if exists gh_ost_test;
delimiter ;;
create event gh_ost_test
  on schedule every 1 second
  starts current_timestamp
  ends current_timestamp + interval 120 second
  on completion not preserve
  enable
  do
begin
  -- Modify the unique key column (code) to exercise UK-modifying UPDATE path
  update gh_ost_test set code = code + 10 where id = 1;
  update gh_ost_test set code = code + 10 where id = 3;
  update gh_ost_test set code = code + 10 where id = 5;
  -- Also do normal updates, inserts, deletes
  update gh_ost_test set name = concat(name, '+') where id = 2;
  insert into gh_ost_test (code, name) values (floor(10000 + rand() * 90000), 'new');
  delete from gh_ost_test where id > 15 order by id desc limit 1;
end ;;
delimiter ;
