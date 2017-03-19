# Shared key

A requirement for a migration to run is that the two _before_ and _after_ tables have a shared unique key. This is to elaborate and illustrate on the matter.

### Introduction

Consider a classic, simple migration. The table is any normal:

```
CREATE TABLE tbl (
  id bigint unsigned not null auto_increment,
  data varchar(255),
  more_data int,
  PRIMARY KEY(id)
)
```

And the migration is a simple `add column ts timestamp`.

In such migration there is no change in indexes, and in particular no change to any unique key, and specifically no change to the `PRIMARY KEY`. To run this migration, `gh-ost` would iterate the `tbl` table using the primary key, copy rows from `tbl` to the _ghost_ table `_tbl_gho` by order of `id`, and then apply binlog events onto `_tbl_gho`.

Applying the binlog events assumes the existence of a shared unique key. For example, an `UPDATE` statement in the binary log translate to a `REPLACE` statement which `gh-ost` applies to the _ghost_ table. Such statement expects to add or replace an existing row based on given row data. In particular, it would _replace_ an existing row if a unique key violation is met.

So `gh-ost` correlates `tbl` and `_tbl_gho` rows using a unique key. In the above example that would be the `PRIMARY KEY`.

### Rules

There must be a shared set of not-null columns for which there is a unique constraint in both the original table and the migration (_ghost_) table.

### Interpreting the rules

The same columns must be covered by a unique key in both tables. This doesn't have to be the `PRIMARY KEY`. This doesn't have to be a key of the same name.

Upon migration, `gh-ost` inspects both the original and _ghost_ table and attempts to find at least one such unique key (or rather, a set of columns) that is shared between the two. Typically this would just be the `PRIMARY KEY`, but sometimes you may change the `PRIMARY KEY` itself, in which case `gh-ost` will look for other options.

`gh-ost` expects unique keys where no `NULL` values are found, i.e. all columns covered by the unique key are defined as `NOT NULL`. This is implicitly true for `PRIMARY KEY`s. If no such key can be found, `gh-ost` bails out. In the event there is no such key, but you happen to _know_ your columns have no `NULL` values even though they're `NULL`-able, you may take responsibility and pass the `--allow-nullable-unique-key`. The migration will run well as long as no `NULL` values are found in the unique key's columns. Any actual `NULL`s may corrupt the migration.

### Examples: allowed and not allowed

```
create table some_table (
  id int auto_increment,
  ts timestamp,
  name varchar(128) not null,
  owner_id int not null,
  loc_id int,
  primary key(id),
  unique key name_uidx(name)
)
```

Following are examples of migrations that are _good to run_:

- `add column i int`
- `add key owner_idx(owner_id)`
- `add unique key owner_name_idx(owner_id, name)` - though you need to make sure to not write conflicting rows while this migration runs
- `drop key name_uidx` - `primary key` is shared between the tables
- `drop primary key, add primary key(owner_id, loc_id)` - `name_uidx` is shared between the tables and is used for migration
- `change id bigint unsigned` - the `'primary key` is used. The change of type still makes the `primary key` workable.
- `drop primary key, drop key name_uidx, create primary key(name), create unique key id_uidx(id)` - swapping the two keys. `gh-ost` is still happy because `id` is still unique in both tables. So is `name`.


Following are examples of migrations that _cannot run_:

- `drop primary key, drop key name_uidx` - no unique key to _ghost_ table, so clearly cannot run
- `drop primary key, drop key name_uidx, create primary key(name, owner_id)` - no shared columns to both tables. Even though `name` exists in the _ghost_ table's `primary key`, it is only part of the key and in itself does not guarantee uniqueness in the _ghost_ table.

Also, you cannot run a migration on a table that doesn't have some form of `unique key` in the first place, such as `some_table (id int, ts timestamp)`
