create user if not exists 'repl'@'%' identified with mysql_native_password by 'repl';
grant replication slave on *.* to 'repl'@'%'; flush privileges;
create user if not exists 'gh-ost'@'%' identified with mysql_native_password by 'gh-ost';
grant all on *.* to 'gh-ost'@'%';

