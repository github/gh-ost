drop table if exists gh_ost_test;
create table gh_ost_test (
  id int auto_increment,
  a int not null,
  virtual_sum int as (a + 10) virtual,
  b int not null,
  stored_sum int as (a + b) stored,
  json_value json default null,
  virtual_json_value varchar(16) as (
    coalesce(json_unquote(json_extract(json_value, '$.value')), 'direct')
  ) virtual,
  primary key(id)
) auto_increment=1;

insert into gh_ost_test (a, b, json_value) values
  (1, 2, json_object('value', 'team')),
  (3, 5, json_object('value', 'project')),
  (8, 13, null);

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
  insert into gh_ost_test (a, b, json_value) values (2, 3, json_object('value', 'team'));
  insert into gh_ost_test (a, b, json_value) values (5, 8, json_object('value', 'project'));
  insert into gh_ost_test (a, b, json_value) values (13, 21, null);
  update gh_ost_test set a=a+1, b=b+2, json_value=json_object('value', 'updated') where id <= 3;
  update gh_ost_test set b=b+1 where id > 3;
  delete from gh_ost_test where id > 3 order by id limit 1;
end ;;
