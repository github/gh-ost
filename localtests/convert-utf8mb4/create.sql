drop table if exists gh_ost_test;
create table gh_ost_test (
  id int auto_increment,
  t varchar(128) charset utf8 collate utf8_general_ci,
  tl varchar(128) charset latin1 not null,
  ta varchar(128) charset ascii not null,
  primary key(id)
) auto_increment=1;

insert into gh_ost_test values (null, 'átesting');


insert into gh_ost_test values (null, 'Hello world, Καλημέρα κόσμε, コンニチハ', 'átesting0', 'initial');

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
  insert into gh_ost_test values (null, md5(rand()), 'átesting-a', 'a');
  insert into gh_ost_test values (null, 'novo proprietário', 'átesting-b', 'b');
  insert into gh_ost_test values (null, '2H₂ + O₂ ⇌ 2H₂O, R = 4.7 kΩ, ⌀ 200 mm', 'átesting-c', 'c');
  insert into gh_ost_test values (null, 'usuário', 'átesting-x', 'x');

  delete from gh_ost_test where ta='x' order by id desc limit 1;
end ;;
