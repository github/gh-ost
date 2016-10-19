# The Fine Print: What are You Not Telling Me?

Here are technical considerations you may be interested in. We write here things that are not an obvious [Requirements & Limitations](requirements-and-limitations.md)

# Connecting to replica

`gh-ost` prefers connecting to a replica. If your master uses Statement Based Replication, this is a _requirement_.

What does "connect to replica" mean?

- `gh-ost` connects to the replica as a normal client
- It additionally connects as a replica to the replica (pretends to be a MySQL replica itself)
- It auto-detects the master

`gh-ost` reads the RBR binary logs from the replica, and applies events onto the master as part of the table migration.

THE FINE PRINT:

- You trust the replica's binary logs to represent events applied on master.
  - If you don't trust the replica, or if you suspect there's data drift between replica & master, take notice. 
  - If the table on the replica has a different schema than the master, `gh-ost` likely won't work correctly.
  - Our take: we trust replica data; if master dies in production, we promote a replica. Our read serving is based on replica(s).
  
- If your master is RBR, do instead connect `gh-ost` to master, via `--allow-on-master` (see [cheatsheet](cheatsheet.md)).
  
- Replication needs to run.
  - This is an obvious, but worth stating. You cannot perform a migration with "connect to replica" if your replica lags. `gh-ost` will actually do all it can so that replication does not lag, and avoid critical operations if replication is lagging.

# Network usage

`gh-ost` reads binary logs and then applies them onto the migrated server.

THE FINE PRINT:

- `gh-ost` delivers more network traffic than other online-schema-change tools, that let MySQL handle all data transfer internally. This is part of the [triggerless design](triggerless-design.md).
  - Our take: we deal with cross-DC migration traffic and this is working well for us.

# Impersonating as a replica

`gh-ost` impersonates as a replica: it connects to a MySQL server, says "oh hey, I'm a replica, please send me binary logs kthx".

THE FINE PRINT:

- `SHOW SLAVE HOSTS` or `SHOW PROCESSLIST` will list this strange "replica" that you can't really connect to.
