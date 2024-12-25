FROM almalinux:9

ARG repo_url
ENV repo_url ${repo_url}

RUN \
  yum -y install https://repo.mysql.com/mysql84-community-release-el9.rpm

RUN \
  yum -y install mysql-community-libs openssl compat-openssl11

RUN \
  curl -o GPG-KEY-MyDumper "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x79EA15C0E82E34BA" && \
  rpm --import GPG-KEY-MyDumper &&\
  echo -e "[mydumper]\nname=MyDumper\nbaseurl=${repo_url}\nenabled=1\ngpgcheck=1" > /etc/yum.repos.d/mydumper.repo &&\
  yum -y install mydumper

# Compilation outputs both mydumper and myloader binaries.
CMD [ "bash", "-c", "echo 'This Docker image contains both mydumper and myloader binaries. Run the container by invoking either mydumper or myloader as first argument.'" ]
