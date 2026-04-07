drop table if exists gh_ost_test;
create table gh_ost_test (
  id int auto_increment,
  email varchar(255) not null,
  primary key (id)
) auto_increment=1;

-- Insert initial data - all unique emails
insert into gh_ost_test (email) values ('alice@example.com');
insert into gh_ost_test (email) values ('bob@example.com');
insert into gh_ost_test (email) values ('charlie@example.com');
