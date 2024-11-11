# Using MyDumper EL9 image
FROM mydumper/mydumper-builder-el9:latest AS builder

# Docker build arguments. Use to customize build.
ARG PERCONA_COMPONENT
ARG CMAKE_ARGS

RUN \
  yum -y install https://repo.mysql.com/mysql84-community-release-el9.rpm

RUN \
  yum -y install mysql-community-libs mysql-community-devel

COPY . /usr/src/
WORKDIR /usr/src/

RUN \
  cmake ${CMAKE_ARGS} . && \
  make && \
  make install

FROM mydumper/mydumper-builder-el9:latest

RUN \
  yum -y install https://repo.mysql.com/mysql84-community-release-el9.rpm

RUN \
  yum -y install zstd mysql-community-libs mysql-community-devel

COPY --from=builder /usr/local/bin /usr/local/bin
COPY --from=builder /etc/mydumper.cnf /etc/

# Compilation outputs both mydumper and myloader binaries.
CMD [ "bash", "-c", "echo 'This Docker image contains both mydumper and myloader binaries. Run the container by invoking either mydumper or myloader as first argument.'" ]
