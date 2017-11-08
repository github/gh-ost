drop table if exists gh_ost_test;
create table gh_ost_test (
  id int auto_increment,
  i int not null,
  primary key(id)
) auto_increment=1;

insert into gh_ost_test values (null, 13);
insert into gh_ost_test values (null, 13);
insert into gh_ost_test values (null, 13);
insert into gh_ost_test values (null, 13);
insert into gh_ost_test values (null, 13);
insert into gh_ost_test values (null, 13);
insert into gh_ost_test values (null, 13);
insert into gh_ost_test values (null, 13);
insert into gh_ost_test values (null, 13);
insert into gh_ost_test values (null, 13);
insert into gh_ost_test values (null, 13);
insert into gh_ost_test values (null, 13);
insert into gh_ost_test values (null, 13);
insert into gh_ost_test values (null, 13);
insert into gh_ost_test values (null, 13);
insert into gh_ost_test values (null, 13);
insert into gh_ost_test values (null, 13);
insert into gh_ost_test values (null, 13);
insert into gh_ost_test values (null, 13);
insert into gh_ost_test values (null, 13);
insert into gh_ost_test values (null, 13);
insert into gh_ost_test values (null, 13);
insert into gh_ost_test values (null, 13);
insert into gh_ost_test values (null, 13);


drop event if exists gh_ost_test;
delimiter ;;
create event gh_ost_test
  on schedule every 1 second
  starts current_timestamp + interval 3 second
  ends current_timestamp + interval 60 second
  on completion not preserve
  enable
  do
begin
  update gh_ost_test set id=-2 where id=21;
end ;;
