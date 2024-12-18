drop table if exists gh_ost_test;
create table gh_ost_test (
  id int auto_increment,
  a int not null,
  b int not null,
  sum_ab int as (a + b) virtual not null,
  primary key(id)
) auto_increment=1;

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
  insert into gh_ost_test (id, a, b) values (null, 2,3);
  insert into gh_ost_test (id, a, b) values (null, 2,4);
  insert into gh_ost_test (id, a, b) values (null, 2,5);
  insert into gh_ost_test (id, a, b) values (null, 2,6);
  insert into gh_ost_test (id, a, b) values (null, 2,7);
  insert into gh_ost_test (id, a, b) values (null, 2,8);
  insert into gh_ost_test (id, a, b) values (null, 2,9);
  insert into gh_ost_test (id, a, b) values (null, 2,0);
  insert into gh_ost_test (id, a, b) values (null, 2,1);
  insert into gh_ost_test (id, a, b) values (null, 2,2);
  update gh_ost_test set b=b+1 where id < 5;
  update gh_ost_test set b=b-1 where id >= 5;
end ;;
