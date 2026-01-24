# Using Debian's GCC image, pinned to latest LTS, scheduled to EOL on Jun'26.
FROM almalinux:10 as builder

ARG DEBIAN_FRONTEND=noninteractive

# Package 'lsb-release' is required by 'percona-release' package.
RUN \
  yum -y install epel-release && \
  yum -y install --allowerasing wget curl && \
  yum -y install cmake gcc-c++ git make glib2-devel zlib-devel openssl-devel libzstd-devel sudo && \
  echo "export PATH=/opt/rh/gcc-toolset-12/root/usr/bin/:$PATH" >> /etc/profile.d/sh.local && \
  yum clean all

RUN \
  yum -y install  https://repo.percona.com/yum/percona-release-latest.noarch.rpm && \
  yum clean all

RUN \
  wget https://r.mariadb.com/downloads/mariadb_repo_setup -O /tmp/mariadb_repo_setup

RUN \
  yum -y install https://repo.mysql.com/mysql84-community-release-el10.rpm && \
  rpm --import https://repo.mysql.com/RPM-GPG-KEY-mysql-2023 && \
  yum clean all

RUN \
  yum install gpg && \
  rpm -e gpg-pubkey-a8d3785c-6536acda && \
  curl -fsSL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0xB7B3B788A8D3785C" -o /tmp/fresh.asc && \
  gpg --import /tmp/fresh.asc && \
  rm /etc/pki/rpm-gpg/RPM-GPG-KEY-mysql-2023 && \
  gpg --output /etc/pki/rpm-gpg/RPM-GPG-KEY-mysql-2023 --export BCA43417C3B485DD128EC6D4B7B3B788A8D3785C
