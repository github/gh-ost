-- Bug #1: Double-transformation in trigger length validation
-- A trigger with a 60-char name should be valid: ghost name = 60 + 4 (_ght) = 64 chars (max allowed).
-- But validateGhostTriggersLength() applies GetGhostTriggerName() twice,
-- computing 60 + 4 + 4 = 68, which falsely exceeds the 64-char limit.

drop table if exists gh_ost_test;

create table gh_ost_test (
  id int auto_increment,
  i int not null,
  primary key(id)
) auto_increment=1;

-- Trigger name is exactly 60 characters (padded to 60).
-- Ghost name with _ght suffix = 64 chars = exactly at the MySQL limit.
-- 60 chars: trigger_long_name_padding_aaaaaaaaaaaaaaaaaaaaa_60chars_xxxx
create trigger trigger_long_name_padding_aaaaaaaaaaaaaaaaaaaaa_60chars_xxxx
  after insert on gh_ost_test for each row
  set @dummy = 1;

insert into gh_ost_test values (null, 11);
insert into gh_ost_test values (null, 13);
insert into gh_ost_test values (null, 17);

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
  insert into gh_ost_test values (null, 23);
  update gh_ost_test set i=i+1 where id=1;
end ;;
delimiter ;
