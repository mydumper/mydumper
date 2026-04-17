# Using AlmaLinux 9
# Full Support Ends: May 31, 2027.
# Maintenance Support Ends: May 31, 2032.
# Extended Life Cycle Support (ELS): June 1, 2032 – May 31, 2035. 

FROM almalinux:9 as builder

ARG DEBIAN_FRONTEND=noninteractive

# Package 'lsb-release' is required by 'percona-release' package.
RUN \
  yum -y install epel-release && \
  yum -y install --allowerasing wget curl && \
  yum -y install cmake gcc-c++ git make glib2-devel zlib-devel pcre-devel openssl-devel libzstd-devel sudo gcc-toolset-12-gcc gcc-toolset-12-gcc-c++ && \
  echo "export PATH=/opt/rh/gcc-toolset-12/root/usr/bin/:$PATH" >> /etc/profile.d/sh.local && \
  yum clean all

RUN \
  yum -y install  https://repo.percona.com/yum/percona-release-latest.noarch.rpm && \
  yum clean all

RUN \
  wget https://r.mariadb.com/downloads/mariadb_repo_setup -O /tmp/mariadb_repo_setup

RUN \
  yum -y install https://repo.mysql.com/mysql84-community-release-el9.rpm && \
  rpm --import https://repo.mysql.com/RPM-GPG-KEY-mysql-2025 && \
  yum clean all
