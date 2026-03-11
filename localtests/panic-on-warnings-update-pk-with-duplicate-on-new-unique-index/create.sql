drop table if exists gh_ost_test;
create table gh_ost_test (
  id int auto_increment,
  email varchar(100) not null,
  primary key (id)
) auto_increment=1;

insert into gh_ost_test (email) values ('alice@example.com');
insert into gh_ost_test (email) values ('bob@example.com');
insert into gh_ost_test (email) values ('charlie@example.com');

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
  -- Keep inserting rows, then updating their PK to create DELETE+INSERT binlog events
  -- Use alice's email so it conflicts with id=1 when applied to ghost table
  insert ignore into gh_ost_test (email) values ('temp@example.com');
  update gh_ost_test set id = 1000 + id, email = 'alice@example.com'
    where email = 'temp@example.com' order by id desc limit 1;
end ;;
