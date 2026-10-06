drop table if exists gh_ost_test;
create table gh_ost_test (
  id int auto_increment,
  i int not null,
  color varchar(32),
  updated tinyint unsigned default 0,
  primary key(id)
) auto_increment=1;

insert into gh_ost_test (i, color) values
  (1,'red'),(2,'green'),(3,'blue'),(4,'orange'),
  (5,'yellow'),(6,'gold'),(7,'silver'),(8,'pink');

-- Multiply to a few hundred rows so that, at chunk-size=10, many chunks are
-- distributed across the parallel workers.
insert into gh_ost_test (i, color) select i, color from gh_ost_test;
insert into gh_ost_test (i, color) select i, color from gh_ost_test;
insert into gh_ost_test (i, color) select i, color from gh_ost_test;
insert into gh_ost_test (i, color) select i, color from gh_ost_test;
insert into gh_ost_test (i, color) select i, color from gh_ost_test;

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
  insert into gh_ost_test (i, color) values (101, 'concurrent');
  update gh_ost_test set updated = 1, color = 'updated' where i = 1 order by id desc limit 1;
  delete from gh_ost_test where i = 2 order by id desc limit 1;
end ;;
