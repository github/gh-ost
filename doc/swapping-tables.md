# Swapping the tables

The table-swap is the final major step of the migration: it's the moment where your original table is pushed aside, and the ghost table (the one we secretly altered and operated on throughout the process) takes its place.

MySQL poses some limitations on how the table swap can take place. While it supports an atomic swap, it does not allow for a swap under controlled lock.

The [facebook OSC](https://www.facebook.com/notes/mysql-at-facebook/online-schema-change-for-mysql/430801045932/) tool documents this nicely. Look for **"Cut-over phase"**.

`gh-ost` supports various types of table-swap / cut-over options:

- `--quick-and-bumpy-swap-tables` - this method is similar to the one taken by the facebook OSC. It's non-blocking but also non-atomic. The original table is first renames and pushed aside, then the ghost table is renamed to take its place. In between the two renames there's a brief period of time where your table just does not exist, and queries will fail.
- Voluntary lock based solution (default at this time): as depicted in [Solving the Facebook-OSC non-atomic table swap problem](http://code.openark.org/blog/mysql/solving-the-facebook-osc-non-atomic-table-swap-problem), this solution uses voluntary MySQL locks, and makes for a blocking swap, where your queries do not fail, but block until operation is complete. This effect is desired. There is danger in this solution, since connection failure of the two sessions involved in creating the lock, would result in a premature swap of the tables, hence with potentially corrupted data.
- We are working at this time on a blocking, safe, atomic solution, using wait conditions and via User Defined Functions which will need to be dynamically loaded onto your MySQL server.
- With [`--test-on-replica`](testing-on-replica.md) there is no table swap.
