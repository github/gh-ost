create table gh_ost_test (
  id int auto_increment,
  json_value json not null,
  primary key(id)
) auto_increment=1;

insert into gh_ost_test (json_value) values
  (json_object('message', 'first', 'nested', json_object('enabled', true))),
  (json_object('message', 'second', 'items', json_array(1, 2, 3))),
  (json_object('message', 'third', 'value', 42));

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
  insert into gh_ost_test (json_value) values
    (json_object('message', 'inserted', 'items', json_array('a', 'b')));
  update gh_ost_test
    set json_value=json_set(json_value, '$.updated', true)
    where id <= 3;
end ;;