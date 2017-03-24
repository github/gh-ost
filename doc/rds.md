# Amazon RDS# Amazon RDS

## Limitations

- No `SUPER` privileges. 
- `gh-ost` runs should be setup use [`--assume-rbr`][assume_rbr_docs] and use `binlog_format=ROW`.
- Aurora does not allow editing of the `read_only` parameter. While it is defined as `{TrueIfReplica}`, the parameter is non-modifiable field.
