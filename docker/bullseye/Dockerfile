# Using Debian's GCC image, pinned to latest LTS
# End of Life End of LTS  End of ELTS 
# 2022-09-10	2024-06-30	2029-06-30
# Reference: https://www.debian.org/releases/
FROM debian:bullseye as builder

ARG DEBIAN_FRONTEND=noninteractive

# bullseye's LTS ended 2026-08-31. deb.debian.org no longer serves it, and
# archive.debian.org has no bullseye-security tree (only up through buster
# as of now) — so point the main suites at the archive and drop the
# security suite line rather than try to repoint it.
RUN sed -i 's|deb.debian.org/debian|archive.debian.org/debian|g' /etc/apt/sources.list && \
    sed -i '/bullseye-security/d' /etc/apt/sources.list && \
    echo 'Acquire::Check-Valid-Until "false";' > /etc/apt/apt.conf.d/99no-check-valid-until

# Package 'lsb-release' is required by 'percona-release' package.
RUN \
  apt-get update && \
  apt-get install -y sudo lsb-release cmake g++ git make libglib2.0-dev zlib1g-dev libpcre3-dev libssl-dev libzstd-dev wget gnupg curl && \
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

RUN \
  . /etc/os-release && \
  curl --fail --location --show-error --silent --output /tmp/mysql-apt-config.deb \
    https://repo.mysql.com/mysql-apt-config.deb \
  && \
  sudo dpkg -i /tmp/mysql-apt-config.deb && \
  rm -v /tmp/mysql-apt-config.deb && \
  sudo apt-get update
