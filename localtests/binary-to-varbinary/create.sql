-- Test for https://github.com/github/gh-ost/issues/909 variant:
-- Binary columns with trailing zeros should preserve their values
-- when migrating from binary(N) to varbinary(M), even for rows
-- modified during migration via binlog events.

drop table if exists gh_ost_test;
create table gh_ost_test (
  id int NOT NULL AUTO_INCREMENT,
  info varchar(255) NOT NULL,
  data binary(20) NOT NULL,
  PRIMARY KEY (id)
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
  -- Insert rows where data has trailing zeros (will be stripped by binlog)
  INSERT INTO gh_ost_test (info, data) VALUES ('insert-during-1', X'aabbccdd00000000000000000000000000000000');
  INSERT INTO gh_ost_test (info, data) VALUES ('insert-during-2', X'11223344556677889900000000000000000000ee');

  -- Update existing rows to values with trailing zeros
  UPDATE gh_ost_test SET data = X'ffeeddcc00000000000000000000000000000000' WHERE info = 'update-target-1';
  UPDATE gh_ost_test SET data = X'aabbccdd11111111111111111100000000000000' WHERE info = 'update-target-2';
end ;;

-- Pre-existing rows (copied via rowcopy, not binlog - these should work fine)
INSERT INTO gh_ost_test (info, data) VALUES
  ('pre-existing-1', X'01020304050607080910111213141516171819ff'),
  ('pre-existing-2', X'0102030405060708091011121314151617181900'),
  ('update-target-1', X'ffffffffffffffffffffffffffffffffffffffff'),
  ('update-target-2', X'eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee');
