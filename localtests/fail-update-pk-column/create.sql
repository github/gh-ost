drop table if exists gh_ost_test;
create table gh_ost_test (
  id int auto_increment,
  i int not null,
  primary key(id)
) auto_increment=1;

insert into gh_ost_test values (null, 101);
insert into gh_ost_test values (null, 102);
insert into gh_ost_test values (null, 103);
insert into gh_ost_test values (null, 104);
insert into gh_ost_test values (null, 105);
insert into gh_ost_test values (null, 106);
insert into gh_ost_test values (null, 107);
insert into gh_ost_test values (null, 108);
insert into gh_ost_test values (null, 109);
insert into gh_ost_test values (null, 110);
insert into gh_ost_test values (null, 111);
insert into gh_ost_test values (null, 112);
insert into gh_ost_test values (null, 113);
insert into gh_ost_test values (null, 114);
insert into gh_ost_test values (null, 115);
insert into gh_ost_test values (null, 116);
insert into gh_ost_test values (null, 117);
insert into gh_ost_test values (null, 118);
insert into gh_ost_test values (null, 119);
insert into gh_ost_test values (null, 120);
insert into gh_ost_test values (null, 121);
insert into gh_ost_test values (null, 122);
insert into gh_ost_test values (null, 123);
insert into gh_ost_test values (null, 124);
insert into gh_ost_test values (null, 125);
insert into gh_ost_test values (null, 126);
insert into gh_ost_test values (null, 127);
insert into gh_ost_test values (null, 128);
insert into gh_ost_test values (null, 129);

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
  update gh_ost_test set id=55 where id=22;
  update gh_ost_test set id=23 where id=23;
  update gh_ost_test set i=5024 where id=24;
end ;;
