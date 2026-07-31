-- gtid_strict_mode enforces strictly in-order sequence numbers per GTID domain.
-- Under --test-on-replica gh-ost writes directly to this replica (ghost table
-- DDL/DML, changelog, cut-over), which would otherwise land in the primary's
-- domain 0 and collide with the replicated 0-1-N stream as out-of-order. Give
-- this server its own domain for locally-originated writes so the two never
-- interleave. Also reset the binlog GTID state left by the local init scripts
-- (create_user.sql ran here too, in domain 0) and start from an empty slave
-- position so the replica cleanly replays the primary's history.
reset master;
set global gtid_slave_pos='';
set global gtid_domain_id=1;
change master to master_host='mysql-primary', master_port=3307, master_user='repl', master_password='repl', master_use_gtid=slave_pos;
start slave;
