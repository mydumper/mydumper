# Using Debian's GCC image, pinned to latest LTS, scheduled to EOL on Jun'26.
FROM almalinux:9 as builder

# Docker build arguments. Use to customize build.
# Example to enable ZSTD:
# $ docker build ---build-arg CMAKE_ARGS='-DWITH_ZSTD=ON' mydumper .
ARG PERCONA_COMPONENT
ARG CMAKE_ARGS

ARG DEBIAN_FRONTEND=noninteractive

# Package 'lsb-release' is required by 'percona-release' package.
RUN \
  yum -y install epel-release && \
  yum -y install --allowerasing wget curl && \
  yum -y install cmake gcc-c++ git make glib2-devel zlib-devel pcre-devel openssl-devel libzstd-devel sudo

RUN \
  yum -y install https://repo.mysql.com/mysql80-community-release-el9.rpm

RUN \
  yum -y install mysql-community-libs mysql-community-devel

COPY . /usr/src/
WORKDIR /usr/src/

RUN \
  cmake ${CMAKE_ARGS} . && \
  make && \
  make install


FROM almalinux:9
ARG DEBIAN_FRONTEND=noninteractive
RUN \
  yum -y install epel-release && \
  yum -y install --allowerasing wget curl && \
  yum -y install cmake gcc-c++ git make sudo

RUN \
  yum -y install https://repo.mysql.com/mysql80-community-release-el9.rpm

RUN \
  yum -y install mysql-community-libs

COPY --from=builder /usr/local/bin /usr/local/bin

# Compilation outputs both mydumper and myloader binaries.
CMD [ "bash", "-c", "echo 'This Docker image contains both mydumper and myloader binaries. Run the container by invoking either mydumper or myloader as first argument.'" ]
