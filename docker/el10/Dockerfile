# Using AlmaLinux 10
# Full Support: Expected to last 5 years from release, typically until 2030.
# Maintenance Support: Expected to last until approximately 2035.
# Extended Life Cycle Support (ELS): Projected until roughly 2038. 

FROM almalinux:10 as builder

ARG DEBIAN_FRONTEND=noninteractive

# Package 'lsb-release' is required by 'percona-release' package.
RUN \
  yum -y install epel-release && \
  yum -y install --allowerasing wget curl && \
  yum -y install cmake gcc-c++ git make glib2-devel zlib-devel openssl-devel libzstd-devel sudo yum-utils && \
  echo "export PATH=/opt/rh/gcc-toolset-12/root/usr/bin/:$PATH" >> /etc/profile.d/sh.local && \
  yum clean all

RUN \
  yum -y install  https://repo.percona.com/yum/percona-release-latest.noarch.rpm && \
  yum clean all

RUN \
  wget https://r.mariadb.com/downloads/mariadb_repo_setup -O /tmp/mariadb_repo_setup

RUN \
  rpm --import https://repo.mysql.com/RPM-GPG-KEY-mysql-2025 && \
  yum clean all
