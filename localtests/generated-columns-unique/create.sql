drop table if exists gh_ost_test;
create table gh_ost_test (
  id int auto_increment,
  `idb` varchar(36) CHARACTER SET utf8mb4 GENERATED ALWAYS AS (json_unquote(json_extract(`jsonobj`,_utf8mb4'$._id'))) STORED NOT NULL,
  `jsonobj` json NOT NULL,
  updated datetime DEFAULT NULL,
  PRIMARY KEY (`id`,`idb`)
) auto_increment=1;

insert into gh_ost_test (id, jsonobj) values (null, '{"_id":2}');
insert into gh_ost_test (id, jsonobj) values (null, '{"_id":3}');

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
  insert into gh_ost_test (id, jsonobj) values (null, '{"_id":5}');
  insert into gh_ost_test (id, jsonobj) values (null, '{"_id":7}');
  insert into gh_ost_test (id, jsonobj) values (null, '{"_id":11}');
  insert into gh_ost_test (id, jsonobj) values (null, '{"_id":13}');
  insert into gh_ost_test (id, jsonobj) values (null, '{"_id":17}');
  insert into gh_ost_test (id, jsonobj) values (null, '{"_id":19}');

  update gh_ost_test set updated=NOW() where idb=5;
  update gh_ost_test set updated=NOW() where idb=7;
  update gh_ost_test set updated=NOW() where idb=11;
  update gh_ost_test set updated=NOW() where idb=13;
  update gh_ost_test set updated=NOW() where idb=17;
  update gh_ost_test set updated=NOW() where idb=19;
end ;;
