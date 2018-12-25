# Shared key

gh-ost requires for every migration that both the _before_ and _after_ versions of the table share the same unique not-null key columns. This page illustrates this rule.

### Introduction

Consider a simple migration, with a normal table,

```sql
CREATE TABLE tbl (
  id bigint unsigned not null auto_increment,
  data varchar(255),
  more_data int,
  PRIMARY KEY(id)
)
```

and the migration `add column ts timestamp`. The _after_ table version would be:

```sql
CREATE TABLE tbl (
  id bigint unsigned not null auto_increment,
  data varchar(255),
  more_data int,
  ts timestamp,
  PRIMARY KEY(id)
)
```

(This is also the definition of the _ghost_ table, except that that table would be called `_tbl_gho`). 

In this migration, the _before_ and _after_ versions contain the same unique not-null key (the PRIMARY KEY). To run this migration, `gh-ost` would iterate through the `tbl` table using the primary key, copy rows from `tbl` to the _ghost_ table `_tbl_gho` in primary key order, while also applying the binlog event writes from `tble` onto `_tbl_gho`.

The applying of the binlog events is what requires the shared unique key. For example, an `UPDATE` statement to `tbl` translates to a `REPLACE` statement which `gh-ost` applies to `_tbl_gho`. A `REPLACE` statement expects to insert or replace an existing row based on its row's values and the table's unique key constraints. In particular, if inserting that row would result in a unique key violation (e.g., a row with that primary key already exists), it would _replace_ that existing row with the new values.

So `gh-ost` correlates `tbl` and `_tbl_gho` rows one to one using a unique key. In the above example that would be the `PRIMARY KEY`.

### Interpreting the rule

The _before_ and _after_ versions of the table share the same unique not-null key, but:
- the key doesn't have to be the PRIMARY KEY
- the key can have a different name between the _before_ and _after_ versions (e.g., renamed via DROP INDEX and ADD INDEX) so long as it contains the exact same column(s)

At the start of the migration, `gh-ost` inspects both the original and _ghost_ table it created, and attempts to find at least one such unique key (or rather, a set of columns) that is shared between the two. Typically this would just be the `PRIMARY KEY`, but some tables don't have primary keys, or sometimes it is the primary key that is being modified by the migration. In these cases `gh-ost` will look for other options.

`gh-ost` expects unique keys where no `NULL` values are found, i.e. all columns contained in the unique key are defined as `NOT NULL`. This is implicitly true for primary keys. If no such key can be found, `gh-ost` bails out. 

If the table contains a unique key with nullable columns, but you know your columns contain no `NULL` values, use the `--allow-nullable-unique-key` option. The migration will run well as long as no `NULL` values are found in the unique key's columns. **Any actual `NULL`s may corrupt the migration.**

### Examples: Allowed and Not Allowed

```sql
create table some_table (
  id int not null auto_increment,
  ts timestamp,
  name varchar(128) not null,
  owner_id int not null,
  loc_id int not null,
  primary key(id),
  unique key name_uidx(name)
)
```

Note the two unique, not-null indexes: the primary key and `name_uidx`.

Allowed migrations:

- `add column i int`
- `add key owner_idx (owner_id)`
- `add unique key owner_name_idx (owner_id, name)` - **be careful not to write conflicting rows while this migration runs**
- `drop key name_uidx` - `primary key` is shared between the tables
- `drop primary key, add primary key(owner_id, loc_id)` - `name_uidx` is shared between the tables
- `change id bigint unsigned not null auto_increment` - the `primary key` changes datatype but not value, and can be used
- `drop primary key, drop key name_uidx, add primary key(name), add unique key id_uidx(id)` - swapping the two keys. Either `id` or `name` could be used

Not allowed:

- `drop primary key, drop key name_uidx` - the _ghost_ table has no unique key
- `drop primary key, drop key name_uidx, create primary key(name, owner_id)` - no shared columns to the unique keys on both tables. Even though `name` exists in the _ghost_ table's `primary key`, it is only part of the key and in itself does not guarantee uniqueness in the _ghost_ table.


### Workarounds

If you need to change your primary key or only not-null unique index to use different columns, you will want to do it as two separate migrations:
1. `ADD UNIQUE KEY temp_pk (temp_pk_column,...)`
1. `DROP PRIMARY KEY, DROP KEY temp_pk, ADD PRIMARY KEY (temp_pk_column,...)`
