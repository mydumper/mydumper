# Using Debian's GCC image, pinned to latest LTS, scheduled to EOL on Jun'26.
FROM centos:7 as builder

ARG DEBIAN_FRONTEND=noninteractive

# Package 'lsb-release' is required by 'percona-release' package.
RUN \
  sed -i 's/mirrorlist/#mirrorlist/g' /etc/yum.repos.d/CentOS-* && \
  sed -i 's|#baseurl=http://mirror.centos.org|baseurl=http://vault.centos.org|g' /etc/yum.repos.d/CentOS-*

RUN \
  yum -y install epel-release && \
  yum -y install wget curl && \
  yum -y install cmake gcc-c++ git make glib2-devel zlib-devel pcre-devel pcre2-devel openssl-devel libzstd-devel sudo && \
  yum clean all

RUN \
  yum -y install  https://repo.percona.com/yum/percona-release-latest.noarch.rpm && \
  yum clean all

RUN \
  wget https://r.mariadb.com/downloads/mariadb_repo_setup -O /tmp/mariadb_repo_setup

RUN \
  yum -y install https://repo.mysql.com/mysql84-community-release-el7.rpm && \
  rpm --import https://repo.mysql.com/RPM-GPG-KEY-mysql-2023 && \
  yum clean all
