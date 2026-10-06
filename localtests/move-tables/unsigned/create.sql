drop table if exists gh_ost_test;
create table gh_ost_test (
  id int auto_increment,
  signed_value bigint not null,
  unsigned_value int unsigned not null,
  unsigned_big_value bigint unsigned not null,
  primary key(id)
) auto_increment=1;

insert into gh_ost_test (signed_value, unsigned_value, unsigned_big_value) values
  (-9223372036854775807, 4294967295, 18446744073709551615),
  (-9223372036854775806, 4294967294, 18446744073709551614),
  (-9223372036854775805, 4294967293, 18446744073709551613);

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
  insert into gh_ost_test (signed_value, unsigned_value, unsigned_big_value) values
    (-9223372036854775804, 4294967292, 18446744073709551612);
  update gh_ost_test
    set signed_value=signed_value+1,
        unsigned_value=unsigned_value-1,
        unsigned_big_value=unsigned_big_value-1
    where id <= 3;
end ;;