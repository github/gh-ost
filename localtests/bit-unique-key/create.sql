drop table if exists gh_ost_test;
create table gh_ost_test (
  `id` bigint not null,
  `bit_col` bit not null,
  primary key (`id`, `bit_col`)
) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
insert into gh_ost_test values (1, b'1');
insert into gh_ost_test values (2, b'1');
insert into gh_ost_test values (3, b'1');
