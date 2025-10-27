# Using Debian's GCC image, pinned to latest LTS, scheduled to EOL on Jun'26.
FROM ubuntu:noble as builder

ARG DEBIAN_FRONTEND=noninteractive

RUN \
  apt-get update && \
  apt-get install -y sudo && \
  apt-get clean

# Package 'lsb-release' is required by 'percona-release' package.
RUN \
  sudo apt-get update && \
  sudo apt-get install -y lsb-release cmake g++ git make libglib2.0-dev zlib1g-dev libpcre3-dev libssl-dev libzstd-dev wget gnupg curl zstd && \
  sudo apt-get clean

RUN \
  . /etc/os-release && \
  curl --fail --location --show-error --silent --output /tmp/percona-release.deb \
    https://repo.percona.com/apt/percona-release_latest.${VERSION_CODENAME}_all.deb \
  && \
  sudo dpkg -i /tmp/percona-release.deb && \
  rm -v /tmp/percona-release.deb && \
  sudo apt-get update 

RUN \
  wget https://r.mariadb.com/downloads/mariadb_repo_setup -O /tmp/mariadb_repo_setup

RUN \
  . /etc/os-release && \
  curl -fsSL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0xB7B3B788A8D3785C" -o /tmp/fresh.asc && \
  gpg --import /tmp/fresh.asc && \
  rm -f /usr/share/keyrings/mysql-apt-config.gpg && \
  gpg --output /usr/share/keyrings/mysql-apt-config.gpg --export BCA43417C3B485DD128EC6D4B7B3B788A8D3785C && \
  curl --fail --location --show-error --silent --output /tmp/mysql-apt-config.deb \
    https://repo.mysql.com/mysql-apt-config.deb && \
  sudo dpkg -i /tmp/mysql-apt-config.deb && \
  rm -v /tmp/mysql-apt-config.deb && \
  sudo apt-get update
