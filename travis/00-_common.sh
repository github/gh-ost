export GOPATH=$PWD/.gopath
BINDIR=$PWD/bin

GHOST_SRC=$GOPATH/src/github.com/github/gh-ost
GHOST_BIN=$BINDIR/gh-ost

MASTER_MYSQL_DIR=/home/travis/.mysql/master
SLAVE_MYSQL_DIR=/home/travis/.mysql/slave

MASTER_MYSQL="mysql -u root -S $MASTER_MYSQL_DIR/mysql.sock"
SLAVE_MYSQL="mysql -u root -S $SLAVE_MYSQL_DIR/mysql.sock"

MASTER_MYSQLD="mysqld --defaults-file=$PWD/travis/mysql-config/master.cnf"
SLAVE_MYSQLD="mysqld --defaults-file=$PWD/travis/mysql-config/slave.cnf"

MASTER_MYSQLADMIN="mysqladmin --user root --socket=$MASTER_MYSQL_DIR/mysql.sock"
SLAVE_MYSQLADMIN="mysqladmin --user root --socket=$SLAVE_MYSQL_DIR/mysql.sock"

# Ensure travis build do not hang as not all processes are terminated if
# the build errors.
trap "travis/05-shutdown-mysql.sh" ERR
