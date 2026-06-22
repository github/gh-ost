create user if not exists 'repl'@'%' identified by 'repl';
grant replication slave on *.* to 'repl'@'%';
create user if not exists 'gh-ost'@'%' identified by 'gh-ost';
grant all on *.* to 'gh-ost'@'%';
flush privileges;
