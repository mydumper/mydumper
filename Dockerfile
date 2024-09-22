# Using Debian's GCC image, pinned to latest LTS, scheduled to EOL on Jun'26.
FROM mydumper/mydumper-builder-el9:latest AS builder

# Docker build arguments. Use to customize build.
# Example to enable ZSTD:
# $ docker build ---build-arg CMAKE_ARGS='-DWITH_ZSTD=ON' mydumper .
ARG PERCONA_COMPONENT
ARG CMAKE_ARGS

ARG DEBIAN_FRONTEND=noninteractive

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
ARG DEBIAN_FRONTEND=noninteractive

RUN \
  yum -y install https://repo.mysql.com/mysql84-community-release-el9.rpm

RUN \
  yum -y install zstd mysql-community-libs mysql-community-devel

COPY --from=builder /usr/local/bin /usr/local/bin
COPY --from=builder /etc/mydumper.cnf /etc/

# Compilation outputs both mydumper and myloader binaries.
CMD [ "bash", "-c", "echo 'This Docker image contains both mydumper and myloader binaries. Run the container by invoking either mydumper or myloader as first argument.'" ]
