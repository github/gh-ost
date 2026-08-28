drop table if exists gh_ost_test;
create table gh_ost_test (
  id int auto_increment,
  i int not null,
  v varchar(128),
  updated tinyint unsigned default 0,
  primary key(id, v)
) auto_increment=1;

insert into gh_ost_test values
  (null, 11, 'eleven', 0),(null, 13, 'thirteen', 0),
  (null, 17, 'seventeen', 0),(null, 19, 'nineteen', 0),
  (null, 23, 'twenty three', 0),(null, 29, 'twenty nine', 0),
  (null, 31, 'thirty one', 0),(null, 37, 'thirty seven', 0);

-- Multiply rows so multiple chunks span the composite primary key and are
-- distributed across parallel workers at chunk-size=10.
insert into gh_ost_test (i, v) select i, v from gh_ost_test;
insert into gh_ost_test (i, v) select i, v from gh_ost_test;
insert into gh_ost_test (i, v) select i, v from gh_ost_test;
insert into gh_ost_test (i, v) select i, v from gh_ost_test;
insert into gh_ost_test (i, v) select i, v from gh_ost_test;

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
  insert into gh_ost_test values (null, 11, 'eleven', 0);
  update gh_ost_test set updated = 1 where i = 11 order by id desc limit 1;
  insert into gh_ost_test values (null, 23, 'twenty three', 0);
  delete from gh_ost_test where i = 31 order by id desc limit 1;
end ;;
