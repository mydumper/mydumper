FROM almalinux:9

ARG tags
ENV tags ${tags}

RUN \
  yum -y install https://repo.mysql.com/mysql84-community-release-el9.rpm && yum clean all

RUN \
  yum -y install zstd mysql-community-libs openssl compat-openssl11 && yum clean all

RUN \
  curl -o GPG-KEY-MyDumper "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x79EA15C0E82E34BA" && \
  echo "TAGS: ${tags}" && \
  rpm --import GPG-KEY-MyDumper &&\
  echo -e "[mydumper]\nname=MyDumper\nbaseurl=https://mydumper.github.io/mydumper/repo/yum/\$releasever/$(if [ $(($(echo "${tags}" | cut -d'.' -f3 | cut -d'-' -f1)%2)) -eq 0 ]; then echo 'testing'; fi)\nenabled=1\ngpgcheck=1" > /etc/yum.repos.d/mydumper.repo &&\
  yum -y install mydumper && yum clean all

# Compilation outputs both mydumper and myloader binaries.
CMD [ "bash", "-c", "echo 'This Docker image contains both mydumper and myloader binaries. Run the container by invoking either mydumper or myloader as first argument.'" ]
