`gh-ost` has been updated to work with Azure Database for MySQL however due to GitHub does not use it, this documentation is community driven so if you find a bug please [open an issue][new_issue]!

# Azure Database for MySQL

## Limitations

- `gh-ost` runs should be setup use [`--assume-rbr`][assume_rbr_docs] and use `binlog_row_image=FULL`.
- Azure Database for MySQL does not use same user name suffix for master and replica, so master host, user and password need to be pointed out. 

## Step
1. Change the replica server's `binlog_row_image` from `MINIMAL` to `FULL`. See [guide](https://docs.microsoft.com/en-us/azure/mysql/howto-server-parameters) on Azure document.
2. Use your `gh-ost` always with additional 5 parameter
```{bash}
gh-ost \
--azure \
--assume-master-host=master-server-dns-name \
--master-user="master-user-name" \
--master-password="master-password" \
--assume-rbr \
[-- other parameters you need]
```


[new_issue]: https://github.com/github/gh-ost/issues/new
[assume_rbr_docs]: https://github.com/github/gh-ost/blob/master/doc/command-line-flags.md#assume-rbr
[migrate_test_on_replica_docs]: https://github.com/github/gh-ost/blob/master/doc/cheatsheet.md#c-migratetest-on-replica
