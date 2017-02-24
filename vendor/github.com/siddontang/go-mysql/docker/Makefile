MYSQL_IMAGE=siddontang/mysql:latest
MARIADB_IMAGE=siddontang/mariadb:latest

all: run_mysql1 run_mysql2 run_mysql3 run_mariadb1 run_mariadb2 run_mariadb3

run_mysql1:
	@docker run -d -p 3306:3306 --name=mysql1 -e "GTID_MODE=on" -e "SERVER_ID=1" ${MYSQL_IMAGE}

run_mysql2:
	@docker run -d -p 3307:3306 --name=mysql2 -e "GTID_MODE=on" -e "SERVER_ID=2" ${MYSQL_IMAGE}

run_mysql3:
	@docker run -d -p 3308:3306 --name=mysql3 -e "GTID_MODE=on" -e "SERVER_ID=3" ${MYSQL_IMAGE}

run_mariadb1:
	@docker run -d -p 3316:3306 --name=mariadb1 -e "SERVER_ID=4" ${MARIADB_IMAGE}

run_mariadb2:
	@docker run -d -p 3317:3306 --name=mariadb2 -e "SERVER_ID=5" ${MARIADB_IMAGE}

run_mariadb3:
	@docker run -d -p 3318:3306 --name=mariadb3 -e "SERVER_ID=6" ${MARIADB_IMAGE}

image_mysql:
	@docker pull ${MYSQL_IMAGE}

image_maridb:
	@docker pull ${MARIADB_IMAGE}

image: image_mysql image_maridb

stop_mysql1:
	@docker stop mysql1

stop_mysql2:
	@docker stop mysql2

stop_mysql3:
	@docker stop mysql3

stop_mariadb1:
	@docker stop mariadb1

stop_mariadb2:
	@docker stop mariadb2

stop_mariadb3:
	@docker stop mariadb3

stop: stop_mysql1 stop_mysql2 stop_mysql3 stop_mariadb1 stop_mariadb2 stop_mariadb3

clean:
	@docker rm -f mysql1 mysql2 mysql3 mariadb1 mariadb2 mariadb3
