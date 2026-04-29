drop event if exists gh_ost_test;
drop table if exists gh_ost_test;
create table gh_ost_test (
  id int auto_increment,
  i int not null,
  ts timestamp default current_timestamp,
  primary key(id)
) auto_increment=1;

-- Insert 200 rows to exercise parallel chunk copying with lag throttle
insert into gh_ost_test (i) values (1),(2),(3),(4),(5),(6),(7),(8),(9),(10);
insert into gh_ost_test (i) select i+10 from gh_ost_test;
insert into gh_ost_test (i) select i+20 from gh_ost_test;
insert into gh_ost_test (i) select i+50 from gh_ost_test;
insert into gh_ost_test (i) select i+100 from gh_ost_test order by id limit 40;

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
  update gh_ost_test set i = i + 1 where id = floor(1 + rand() * 100);
  insert into gh_ost_test (i) values (floor(rand() * 10000));
  delete from gh_ost_test where id > 200 order by id desc limit 1;
end ;;
delimiter ;
