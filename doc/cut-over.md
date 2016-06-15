# Cut-over step

The cut-over is the final major step of the migration: it's the moment where your original table is pushed aside, and the ghost table (the one we secretly altered and operated on throughout the process) takes its place.

MySQL poses some limitations on how the table swap can take place. While it supports an atomic swap, it does not allow for a swap under controlled lock.

The [facebook OSC](https://www.facebook.com/notes/mysql-at-facebook/online-schema-change-for-mysql/430801045932/) tool documents this nicely. Look for **"Cut-over phase"**.

`gh-ost` supports various types of cut-over options:

- `--cut-over=safe` - this is the default value if not specified otherwise. The "safe" cut-over algorithm is depicted [here](https://github.com/github/gh-ost/issues/65). It is essentially a multi-step, locking solution, where queries block until table swapping is complete -- when all operates normally. If, for some reason, connections participating in the cut-over are killed throughout the process, the worst-case scenario is a table-outage: a brief moment where the table ceases to exist, followed by an immediate rollback.
- `--cut-over=two-step` - this method is similar to the one taken by the facebook OSC. It's non-blocking but also non-atomic. The original table is first renames and pushed aside, then the ghost table is renamed to take its place. In between the two renames there's a brief period of time where your table just does not exist, and queries will fail.

Also note:
- With `--migrate-on-replica` the cut-over is executed in exactly the same way as on master.
- With `--test-on-replica` the replication is first stopped; then the cut-over is executed just as on master, but then reverted (tables rename forth then back again).
