drop event if exists gh_ost_test;
drop table if exists gh_ost_test;
create table gh_ost_test (
  a int not null,
  b varchar(64) not null default '',
  c int not null,
  primary key(a, c)
);

insert into gh_ost_test values (1, 'row1', 10), (1, 'row2', 20), (2, 'row3', 10), (2, 'row4', 20);
insert into gh_ost_test values (3, 'row5', 30), (4, 'row6', 40), (5, 'row7', 50), (6, 'row8', 60);

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
  update gh_ost_test set b = concat(b, '+') where a = 1 and c = 10;
  update gh_ost_test set b = concat(b, '-') where a = 2 and c = 20;
  insert ignore into gh_ost_test values (7 + floor(rand()*100), 'new', 70 + floor(rand()*100));
  delete from gh_ost_test where a > 10 limit 1;
end ;;
delimiter ;
