
# Changing a column to a stricter data type

`gh-ost` copies existing rows into the ghost table using `INSERT IGNORE`.
This is intentional: it lets the row copy and the binlog replay write the
same rows without failing on duplicate keys
(see [#469](https://github.com/github/gh-ost/issues/469)).

A side effect is that `IGNORE` also downgrades data conversion errors to
warnings, even though `gh-ost` runs its sessions with `STRICT_ALL_TABLES`.
When an `ALTER` narrows a column's type, values that do not fit are
converted or truncated, and the migration completes successfully.

A plain `ALTER TABLE` in strict mode raises an error for the same data.

### Example

Changing a `VARCHAR(64)` column to `BIGINT` with
`--alter="MODIFY job_id BIGINT NOT NULL"` (gh-ost 1.1.11, MySQL 8.0.46,
`sql_mode` including `STRICT_TRANS_TABLES`):

| Original value           | Plain `ALTER TABLE` (strict)  | Value after `gh-ost` (default) |
|--------------------------|-------------------------------|--------------------------------|
| `'abc'`                  | ERROR 1366 (incorrect value)  | `0`                            |
| `'1003x'`                | ERROR 1265 (data truncated)   | `1003`                         |
| `''`                     | ERROR 1366 (incorrect value)  | `0`                            |
| `'99999999999999999999'` | ERROR 1264 (out of range)     | `9223372036854775807`          |

Each row-copy chunk runs a statement of this form:

```sql
insert /* gh-ost `db`.`tbl` */ ignore into `db`.`_tbl_gho` (...)
  (select ... from `db`.`tbl` force index (`PRIMARY`) where ... for share nowait)
```

Some values are converted without any error or warning even by a plain
`ALTER TABLE` in strict mode. For example, `' 1004 '` becomes `1004` and
`'12.7'` becomes `13`.

### Protecting your data

- **Use `--panic-on-warnings`.** `gh-ost` then aborts when the row copy
  produces warnings, and the original table is left unchanged. In the
  example above, the migration stopped with:

		ERROR ApplyIterationInsertQuery failed because of SQL warnings:
		[Warning: Incorrect integer value: 'abc' for column 'job_id' at row 1 (1366);
		Warning: Data truncated for column 'job_id' at row 2 (1265); ...]
  With clean data, the same migration completed normally.

- **Check your data before migrating,** because some conversions produce
  no warning at all. For example, before converting a `VARCHAR` column to
  `BIGINT`:

```sql
SELECT COUNT(*) FROM your_table
WHERE your_column NOT REGEXP '^[0-9]{1,18}\$';
```

If this returns anything other than 0, review the rows first.
