-- Three tables with distinct schemas, primary-key types, and row counts. This
-- exercises the multi-table move-tables path at its widest: per-table
-- runtime state, per-table query builders, interleaved row copy where the tables
-- finish at different times, and a single atomic multi-table RENAME at cutover.
--
-- These three tables are the canonical superset used by the manual harness
-- (script/move-tables/setup, reset, insert-source-primary-loop). The `single`
-- and `multiple-two` localtest fixtures move subsets of them.

drop table if exists gh_ost_test;
create table gh_ost_test (
  id bigint(20) NOT NULL AUTO_INCREMENT,
  column1 int(11) NOT NULL,
  column2 smallint(5) unsigned NOT NULL,
  column3 mediumint(8) unsigned NOT NULL,
  column4 tinyint(3) unsigned NOT NULL,
  column5 int(11) NOT NULL,
  column6 int(11) NOT NULL,
  PRIMARY KEY (id),
  KEY c12_ix (column1, column2)
) auto_increment=1;

insert into gh_ost_test values
  (NULL, 1001, 100, 500000, 10, 1700000001, 1700000002),
  (NULL, 1002, 200, 600000, 20, 1700000003, 1700000004),
  (NULL, 1003, 300, 700000, 30, 1700000005, 1700000006),
  (NULL, 1004, 400, 800000, 40, 1700000007, 1700000008),
  (NULL, 1005, 500, 900000, 50, 1700000009, 1700000010),
  (NULL, 1006, 600, 1000000, 60, 1700000011, 1700000012),
  (NULL, 1007, 700, 1100000, 70, 1700000013, 1700000014),
  (NULL, 1008, 800, 1200000, 80, 1700000015, 1700000016),
  (NULL, 1009, 900, 1300000, 90, 1700000017, 1700000018),
  (NULL, 1010, 1000, 1400000, 100, 1700000019, 1700000020),
  (NULL, 1011, 1100, 1500000, 110, 1700000021, 1700000022),
  (NULL, 1012, 1200, 1600000, 120, 1700000023, 1700000024),
  (NULL, 1013, 1300, 1700000, 130, 1700000025, 1700000026),
  (NULL, 1014, 1400, 1800000, 140, 1700000027, 1700000028),
  (NULL, 1015, 1500, 1900000, 150, 1700000029, 1700000030),
  (NULL, 1016, 1600, 2000000, 160, 1700000031, 1700000032),
  (NULL, 1017, 1700, 2100000, 170, 1700000033, 1700000034),
  (NULL, 1018, 1800, 2200000, 180, 1700000035, 1700000036),
  (NULL, 1019, 1900, 2300000, 190, 1700000037, 1700000038),
  (NULL, 1020, 2000, 2400000, 200, 1700000039, 1700000040),
  (NULL, 1021, 2100, 2500000, 210, 1700000041, 1700000042),
  (NULL, 1022, 2200, 2600000, 220, 1700000043, 1700000044),
  (NULL, 1023, 2300, 2700000, 230, 1700000045, 1700000046),
  (NULL, 1024, 2400, 2800000, 240, 1700000047, 1700000048),
  (NULL, 1025, 2500, 2900000, 250, 1700000049, 1700000050);

drop table if exists gh_ost_test_other;
create table gh_ost_test_other (
  uid int(11) NOT NULL,
  name varchar(64) NOT NULL,
  amount decimal(10,2) NOT NULL,
  created_at datetime NOT NULL,
  PRIMARY KEY (uid),
  UNIQUE KEY name_uq (name)
);

insert into gh_ost_test_other values
  (1,  'alpha',   10.50, '2024-01-01 10:00:00'),
  (2,  'bravo',   20.75, '2024-01-02 11:00:00'),
  (3,  'charlie', 30.00, '2024-01-03 12:00:00'),
  (4,  'delta',   40.25, '2024-01-04 13:00:00'),
  (5,  'echo',    50.50, '2024-01-05 14:00:00'),
  (6,  'foxtrot', 60.75, '2024-01-06 15:00:00'),
  (7,  'golf',    70.00, '2024-01-07 16:00:00'),
  (8,  'hotel',   80.25, '2024-01-08 17:00:00'),
  (9,  'india',   90.50, '2024-01-09 18:00:00'),
  (10, 'juliet', 100.75, '2024-01-10 19:00:00'),
  (11, 'kilo',   110.00, '2024-01-11 20:00:00'),
  (12, 'lima',   120.25, '2024-01-12 21:00:00');

drop table if exists gh_ost_test_third;
create table gh_ost_test_third (
  code varchar(32) NOT NULL,
  label varchar(128) NOT NULL,
  score double NOT NULL,
  updated_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (code),
  KEY score_ix (score)
);

insert into gh_ost_test_third (code, label, score) values
  ('code_1', 'label_1', 1.5),
  ('code_2', 'label_2', 2.5),
  ('code_3', 'label_3', 3.5),
  ('code_4', 'label_4', 4.5),
  ('code_5', 'label_5', 5.5),
  ('code_6', 'label_6', 6.5),
  ('code_7', 'label_7', 7.5),
  ('code_8', 'label_8', 8.5);
