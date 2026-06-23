change master to master_host='mysql-primary', master_port=3307, master_user='repl', master_password='repl', master_use_gtid=slave_pos;
start slave;
