drop table if exists gh_ost_test;
create table gh_ost_test (
  id int auto_increment,
  i int not null,
  updated tinyint not null default 0,
  j json,
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
  insert into gh_ost_test (id, i, j) values (null, 11, '"sometext"');
  insert into gh_ost_test (id, i, j) values (null, 13, '{"key":"val"}');
  insert into gh_ost_test (id, i, j) values (null, 17, '{"is-it": true, "count": 3, "elements": []}');

  update gh_ost_test set j = '{"updated": 11}', updated = 1 where i = 11 and updated = 0;
  update gh_ost_test set j = json_set(j, '$.count', 13, '$.id', id), updated = 1 where i = 13 and updated = 0;
  delete from gh_ost_test where i = 17;
end ;;
