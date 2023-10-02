# Using Debian's GCC image, pinned to latest LTS, scheduled to EOL on Jun'26.
FROM debian:buster as builder

ARG DEBIAN_FRONTEND=noninteractive

# Package 'lsb-release' is required by 'percona-release' package.
RUN \
  apt-get update && \
  apt-get install -y sudo lsb-release cmake g++ git make libglib2.0-dev zlib1g-dev libpcre3-dev libssl-dev libzstd-dev libjemalloc-dev wget gnupg curl && \
  apt-get clean

RUN \
  . /etc/os-release && \
  curl --fail --location --show-error --silent --output /tmp/percona-release.deb \
    https://repo.percona.com/apt/percona-release_latest.${VERSION_CODENAME}_all.deb \
  && \
  dpkg -i /tmp/percona-release.deb && \
  rm -v /tmp/percona-release.deb && \
  apt-get update 

RUN \
  wget https://r.mariadb.com/downloads/mariadb_repo_setup -O /tmp/mariadb_repo_setup


