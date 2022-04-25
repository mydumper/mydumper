# Using Debian's GCC image, pinned to latest LTS, scheduled to EOL on Jun'26.
FROM gcc:11-bullseye as builder

# Docker build arguments. Use to customize build.
# Example to enable ZSTD:
# $ docker build ---build-arg CMAKE_ARGS='-DWITH_ZSTD=ON' mydumper .
ARG PERCONA_COMPONENT
ARG CMAKE_ARGS

ARG DEBIAN_FRONTEND=noninteractive

# Package 'lsb-release' is required by 'percona-release' package.
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
  rm -v /tmp/percona-release.deb && \
  percona-release show

# Temp fix required due to 'percona-release' failing to detect package repos.
RUN \
  sed -i 's/curl -Is/curl -ILs/g' /usr/bin/percona-release && \
  sed -i 's/http:/https:/g' /usr/bin/percona-release && \
  grep http /usr/bin/percona-release

RUN \
  percona-release enable-only ps-80 ${PERCONA_COMPONENT:-release} && \
  apt-get update && \
  apt-get install -y \
    libglib2.0-dev zlib1g-dev libpcre3-dev libssl-dev cmake g++ \
    libperconaserverclient21-dev libperconaserverclient21 libzstd-dev \
  && \
  apt-get clean && \
  rm -rf /var/lib/apt/lists/

COPY . /usr/src/
WORKDIR /usr/src/

RUN \
  cmake ${CMAKE_ARGS} . && \
  make && \
  make install


FROM debian:bullseye
ARG DEBIAN_FRONTEND=noninteractive
RUN \
  apt-get update && \
  apt-get install -y curl lsb-release gnupg libglib2.0 && \
  apt-get clean && \
  rm -rf /var/lib/apt/lists/

RUN \
  . /etc/os-release && \
  curl --fail --location --show-error --silent --output /tmp/percona-release.deb \
    https://repo.percona.com/apt/percona-release_latest.${VERSION_CODENAME}_all.deb \
  && \
  dpkg -i /tmp/percona-release.deb && \
  rm -v /tmp/percona-release.deb && \
  percona-release show

# Temp fix required due to 'percona-release' failing to detect package repos.
RUN \
  sed -i 's/curl -Is/curl -ILs/g' /usr/bin/percona-release && \
  sed -i 's/http:/https:/g' /usr/bin/percona-release

RUN \
  percona-release enable-only ps-80 ${PERCONA_COMPONENT:-release} && \
  apt-get update && \
  apt-get install -y libperconaserverclient21 && \
  apt-get clean && \
  rm -rf /var/lib/apt/lists/

COPY --from=builder /usr/local/bin /usr/local/bin

# Compilation outputs both mydumper and myloader binaries.
CMD [ "bash", "-c", "echo 'This Docker image contains both mydumper and myloader binaries. Run the container by invoking either mydumper or myloader as first argument.'" ]
