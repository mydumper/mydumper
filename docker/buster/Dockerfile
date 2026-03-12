# Using Debian's GCC image, pinned to latest LTS
# End of Life	End of LTS	End of ELTS
# 2022-09-10  2024-06-30  2029-06-30
# Reference: https://www.debian.org/releases/
FROM debian:buster as builder

ARG DEBIAN_FRONTEND=noninteractive

# Package 'lsb-release' is required by 'percona-release' package.
RUN \
  apt-get update && \
  apt-get install -y sudo lsb-release cmake g++ git make libglib2.0-dev zlib1g-dev libpcre3-dev libpcre2-dev libssl-dev libzstd-dev wget gnupg curl && \
  apt-get clean

RUN \
  . /etc/os-release && \
  curl --fail --location --show-error --silent --output /tmp/percona-release.deb \
    https://repo.percona.com/apt/percona-release_latest.${VERSION_CODENAME}_all.deb && \
  dpkg -i /tmp/percona-release.deb && \
  rm -v /tmp/percona-release.deb && \
  apt-get update && \
  sudo apt-get clean 

RUN \
  wget https://r.mariadb.com/downloads/mariadb_repo_setup -O /tmp/mariadb_repo_setup


