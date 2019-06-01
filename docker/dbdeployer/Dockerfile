FROM buildpack-deps:stretch

RUN apt update && apt install -y libaio1 libnuma1

ENV DBDEPLOYER_VERSION=1.30.1 \
    DB_DEPLOYER_SHA256=b54eb047fdea2d3bece820c8c309f6610cfcea858a268b6c51be7ee53c9812bc

RUN origin=https://github.com/datacharmer/dbdeployer/releases/download/v$DBDEPLOYER_VERSION \
  && tarball=dbdeployer-$DBDEPLOYER_VERSION.linux.tar.gz \
  && mkdir /dbdeployer \
  && cd /dbdeployer \
  && wget --progress=bar:force:noscroll $origin/$tarball \
  && sha256sum $tarball | grep $DBDEPLOYER_VERSION \
  && tar -xzf $tarball \
  && chmod +x dbdeployer-$DBDEPLOYER_VERSION.linux \
  && mv dbdeployer-$DBDEPLOYER_VERSION.linux /usr/local/bin/dbdeployer \
  && cd / \
  && rm -rf /dbdeployer

ARG MYSQL_VERSION
ENV MYSQL_VERSION=${MYSQL_VERSION:-5.7.21}

# RUN wget --progress=bar:force:noscroll https://github.com/github/gh-ost-ci-env/raw/master/mysql-tarballs/${MYSQL_VERSION}.tar.gz
COPY ${MYSQL_VERSION}.tar.gz /${MYSQL_VERSION}.tar.gz
RUN mkdir -p /sandbox/binary \
  && mkdir /sandboxes \
  && dbdeployer unpack ${MYSQL_VERSION}.tar.gz \
    --unpack-version=${MYSQL_VERSION} \
    --flavor mysql \
    --sandbox-binary /sandbox/binary/ \
  && dbdeployer deploy replication ${MYSQL_VERSION} \
    --nodes 2 \
    --sandbox-binary /sandbox/binary \
    --sandbox-home /sandboxes/ \
    --sandbox-directory rsandbox \
    --base-port 3306 \
    --gtid \
    --my-cnf-options bind_address=0.0.0.0 \
    --my-cnf-options log_slave_updates \
    --my-cnf-options log_bin \
    --my-cnf-options binlog_format=ROW \
  && /sandboxes/rsandbox/test_replication \
  && /sandboxes/rsandbox/m -uroot -e "grant all on *.* to 'gh-ost'@'%' identified by 'gh-ost'; flush privileges"

EXPOSE 3307 3308

COPY start.sh /usr/local/bin/start.sh
CMD ["/usr/local/bin/start.sh"]
