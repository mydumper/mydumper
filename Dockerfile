FROM gcc:11-bullseye

ARG DEBIAN_FRONTEND=noninteractive

RUN \
  apt-get update && \
  apt-get install -y lsb-release && \
  apt-get clean && \
  rm -rf /var/lib/apt/lists/

RUN \
  . /etc/os-release && \
  curl --fail --location --show-error --silent --output /tmp/percona-release.deb \
    https://repo.percona.com/apt/percona-release_latest.${VERSION_CODENAME}_all.deb \
  && \
  dpkg -i /tmp/percona-release.deb && \
  rm -v /tmp/percona-release.deb

RUN \
  apt-get update && \
  apt-get install -y \
    libglib2.0-dev zlib1g-dev libpcre3-dev libssl-dev cmake g++ libperconaserverclient20-dev libperconaserverclient20 \
  && \
  apt-get clean && \
  rm -rf /var/lib/apt/lists/

COPY . /usr/src/
WORKDIR /usr/src/

RUN \
  cmake . && \
  make && \
  make install
