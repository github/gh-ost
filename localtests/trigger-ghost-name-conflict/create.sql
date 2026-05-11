-- Bug #3 regression test: validateGhostTriggersDontExist must check whole schema
-- MySQL trigger names are unique per SCHEMA, not per table.
-- The validation must detect a trigger with the ghost name on ANY table in the schema.

drop trigger if exists gh_ost_test_ai_ght;
drop trigger if exists gh_ost_test_ai;
drop table if exists gh_ost_test_other;
drop table if exists gh_ost_test;

create table gh_ost_test (
  id int auto_increment,
  i int not null,
  primary key(id)
) auto_increment=1;

-- This trigger has the _ght suffix (simulating a previous migration left it).
-- Ghost name = "gh_ost_test_ai" (suffix removed).
create trigger gh_ost_test_ai_ght
  after insert on gh_ost_test for each row
  set @dummy = 1;

-- Create ANOTHER table with a trigger named "gh_ost_test_ai" (the ghost name).
-- Validation must detect this conflict even though the trigger is on a different table.
create table gh_ost_test_other (
  id int auto_increment,
  primary key(id)
);

create trigger gh_ost_test_ai
  after insert on gh_ost_test_other for each row
  set @dummy = 1;

insert into gh_ost_test values (null, 11);
insert into gh_ost_test values (null, 13);
insert into gh_ost_test values (null, 17);
