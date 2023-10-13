# Using Debian's GCC image, pinned to latest LTS, scheduled to EOL on Jun'26.
FROM cimg/base:current-18.04 as builder

ARG DEBIAN_FRONTEND=noninteractive

# Package 'lsb-release' is required by 'percona-release' package.
RUN \
  sudo apt-get update && \
  sudo apt-get remove libmariadb-dev && \
  sudo apt-get install -y lsb-release cmake g++ git make libglib2.0-dev zlib1g-dev libpcre3-dev libssl-dev libzstd-dev libjemalloc-dev wget gnupg curl && \
  sudo apt-get clean

RUN \
  wget https://download.gnome.org/sources/glib/2.68/glib-2.68.4.tar.xz -P /tmp/src/ && \
  tar xJf /tmp/src/glib-2.68.4.tar.xz -C /tmp/src && \
  sudo apt-get install python3.7 && \
  sudo unlink /usr/bin/python3 && \
  sudo ln /usr/bin/python3.7 /usr/bin/python3 && \
  sudo apt-get install python3-pip && \
  sudo pip3 install --upgrade scikit-build && \
  sudo pip3 install --upgrade meson && \
  sudo pip3 install --upgrade ninja

RUN \
  cd /tmp/src/glib-2.68.4 && \
  /usr/local/bin/meson _build && \
  ninja -C _build && \
  sudo ninja -C _build install


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


