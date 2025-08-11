#!/bin/bash

# TODO: make ASAN counterpart of builders

declare -A all_vendors
vendor=mysql80
all_vendors[${vendor}_0]="$vendor"
all_vendors[${vendor}_1]="mysql:8.0"
all_vendors[${vendor}_3]="mysqlclient"
all_vendors[${vendor}_4]="mysqlclient"

vendor=mysql84
all_vendors[${vendor}_0]="$vendor"
all_vendors[${vendor}_1]="mysql:8.4"
all_vendors[${vendor}_3]="mysqlclient"
all_vendors[${vendor}_4]="mysqlclient"

vendor=percona57
all_vendors[${vendor}_0]="$vendor"
all_vendors[${vendor}_1]="percona:5.7"
all_vendors[${vendor}_3]="perconaserverclient"
all_vendors[${vendor}_4]="perconaserverclient"

vendor=percona80
all_vendors[${vendor}_0]="$vendor"
all_vendors[${vendor}_1]="percona:8"
all_vendors[${vendor}_3]="perconaserverclient"
all_vendors[${vendor}_4]="perconaserverclient"

vendor=mariadb1006
all_vendors[${vendor}_0]="$vendor"
all_vendors[${vendor}_1]="mariadb:10.6"
all_vendors[${vendor}_2]="mariadb-10.6"
all_vendors[${vendor}_3]="mariadb"
all_vendors[${vendor}_4]="mariadbclient"

vendor=mariadb1011
all_vendors[${vendor}_0]="$vendor"
all_vendors[${vendor}_1]="mariadb:10.11"
all_vendors[${vendor}_2]="mariadb-10.11"
all_vendors[${vendor}_3]="mariadb"
all_vendors[${vendor}_4]="mariadbclient"

vendor=tidb
all_vendors[${vendor}_0]="$vendor"
all_vendors[${vendor}_1]="pingcap/tidb"

vendor=debian_default
all_vendors[${vendor}_0]="$vendor"
all_vendors[${vendor}_1]="mysql:8.4"
all_vendors[${vendor}_3]="mariadb"
all_vendors[${vendor}_4]="mariadbclient"

vendor=ubuntu_default
all_vendors[${vendor}_0]="$vendor"
all_vendors[${vendor}_1]="mysql:8.4"
all_vendors[${vendor}_3]="mysqlclient"
all_vendors[${vendor}_4]="mysqlclient"

#_0: name
#_1: docker image
#_2
#_3: variable found in CMAKE, like: -DMYSQL_LIBRARIES_${all_vendors[${vendor}_3]}
#_4: static library name, like: lib${all_vendors[${vendor}_4]}.a


list_mysql_version=( "mysql80" "mysql84" "debian_default" "ubuntu_default")
list_percona_version=( "percona57" "percona80" )
list_mariadb_version=( "mariadb1011" "mariadb1006")
list_all_vendors=( "${list_mysql_version[@]}" "${list_percona_version[@]}" "${list_mariadb_version[@]}" )
list_arch=( "arm" "amd" )
# list_arch=( "amd" )
declare -A all_arch
arch=arm
all_arch[${arch}_resource_class]="arm.medium"
all_arch[${arch}_rpm]="aarch64"
all_arch[${arch}_deb]="arm64"

arch=amd
all_arch[${arch}_resource_class]="large"
all_arch[${arch}_rpm]="x86_64"
all_arch[${arch}_deb]="amd64"

declare -A all_os
os=bionic
all_os[${os}_0]="bionic"
all_os[${os}_1]="cimg/base:current-18.04"
all_os[${os}_2]="percona-release_latest.bionic_all.deb"
all_os[${os}_3]="false"

os=focal
all_os[${os}_0]="focal"
all_os[${os}_1]="cimg/base:stable-20.04"
all_os[${os}_2]="percona-release_latest.focal_all.deb"
all_os[${os}_3]="true"

os=jammy
all_os[${os}_0]="jammy"
all_os[${os}_1]="cimg/base:current-22.04"
all_os[${os}_2]="percona-release_latest.jammy_all.deb"
all_os[${os}_3]="true"

os=noble
all_os[${os}_0]="noble"
all_os[${os}_1]="cimg/base:current-24.04"
all_os[${os}_2]="percona-release_latest.noble_all.deb"
all_os[${os}_3]="true"

os=el7
all_os[${os}_0]="el7"
all_os[${os}_1]="centos:7"

all_os[${os}_3]="true"

os=el8
all_os[${os}_0]="el8"
all_os[${os}_1]="almalinux:8"

all_os[${os}_3]="true"

os=el9
all_os[${os}_0]="el9"
all_os[${os}_1]="almalinux:9"

all_os[${os}_3]="true"

os=bullseye
all_os[${os}_0]="bullseye"
all_os[${os}_1]="debian:bullseye"
all_os[${os}_2]="percona-release_latest.bullseye_all.deb"
all_os[${os}_3]="true"

os=bookworm
all_os[${os}_0]="bookworm"
all_os[${os}_1]="debian:bookworm"
all_os[${os}_2]="percona-release_latest.bookworm_all.deb"
all_os[${os}_3]="true"

os=trixie
all_os[${os}_0]="trixie"
all_os[${os}_1]="debian:trixie"
all_os[${os}_2]="percona-release_latest.trixie_all.deb"
all_os[${os}_3]="true"

# os=
# all_os[${os}_0]=""
# all_os[${os}_1]=""
list_el_os=("el7" "el8" "el9")
list_el_os_without_el=( "7" "8" "9" )
list_ubuntu_os=("bionic" "focal" "jammy" "noble")
list_debian_os=("bullseye" "bookworm" "trixie")
list_all_os=("bionic" "focal" "jammy" "noble" "el7" "el8" "el9" "bullseye" "bookworm" "trixie")

build_man_os="jammy_percona80_amd64"

list_build=(
  "bionic_percona80_amd64"   
  "focal_percona80_amd64"   # "focal_mariadb1011_arm64"
  "jammy_percona80_amd64"   # "jammy_mariadb1011_arm64"
  "noble_mysql84_amd64"         "noble_ubuntu_default_arm64"
  "el7_percona57_x86_64" 
  "el8_mysql84_x86_64"        "el8_mysql84_aarch64"
  "el9_mysql84_x86_64"        "el9_mysql84_aarch64"
  "bullseye_percona80_amd64" 
  "bookworm_mysql84_amd64"    "bookworm_mariadb1011_arm64"
  "trixie_debian_default_amd64" "trixie_debian_default_arm64"
)

#   "noble_percona57"    "noble_percona80"    "noble_mariadb1011"    "noble_mariadb1006"
list_compile=(
  "bionic_percona57"   "bionic_percona80"
  "focal_percona57"    "focal_percona80"    "focal_mariadb1011"    #"focal_mariadb1006"
# jammy is in the tests list 
#                                                                                          "noble_mysql84" This is already on the list of test
                                                                                                             "noble_ubuntu_default"
  "el7_percona57"      "el7_percona80"      "el7_mariadb1011"      "el7_mariadb1006"      "el7_mysql84"
  "el8_percona57"      "el8_percona80"      "el8_mariadb1011"      "el8_mariadb1006"      "el8_mysql84"
                       "el9_percona80"      "el9_mariadb1011"      "el9_mariadb1006"      "el9_mysql84"
  "bullseye_percona57" "bullseye_percona80" "bullseye_mariadb1011" "bullseye_mariadb1006"
  "bookworm_percona57" "bookworm_percona80" "bookworm_mariadb1011"                        "bookworm_mysql84"
                                                                                                             "trixie_debian_default"

)

list_test=("jammy_percona57" "jammy_percona80" "jammy_mariadb1011" "jammy_mariadb1006" "noble_mysql84")

echo "---
version: 2.1

orbs:
  capture-tag: rvla/capture-tag@0.0.2

executors:"

# EXECUTORS



for os in ${list_all_os[@]}
do
## EXECUTORS BY VENDOR
    for vendor in ${list_mariadb_version[@]} ${list_percona_version[@]} tidb
    do
        echo "
  ${all_os[${os}_0]}_${all_vendors[${vendor}_0]}:
    docker:
    - image: mydumper/mydumper-builder-${all_os[${os}_0]}
      environment:
        MYSQL_HOST: 127.0.0.1
        MYSQL_DB: mate
        MYSQL_USER: root
        MYSQL_ALLOW_EMPTY_PASSWORD: true
        MYSQL_PASSWORD:
    - image: ${all_vendors[${vendor}_1]}
      command: mysqld
      environment:
        MYSQL_USER: root
        MYSQL_ALLOW_EMPTY_PASSWORD: true
    working_directory: /tmp/src/mydumper"
    done

    for vendor in ${list_mysql_version[@]}
    do
        echo "
  ${all_os[${os}_0]}_${all_vendors[${vendor}_0]}:
    docker:
    - image: mydumper/mydumper-builder-${all_os[${os}_0]}
      environment:
        MYSQL_HOST: 127.0.0.1
        MYSQL_DB: mate
        MYSQL_ALLOW_EMPTY_PASSWORD: true
    - image: ${all_vendors[${vendor}_1]}
      command: mysqld
      environment:
        MYSQL_ALLOW_EMPTY_PASSWORD: true
    working_directory: /tmp/src/mydumper"
    done
## SINGLE EXECUTOR: used just for building 
                echo "
  ${all_os[${os}_0]}:
    docker:
    - image: mydumper/mydumper-builder-${all_os[${os}_0]}
      environment:
        MYSQL_HOST: 127.0.0.1
        MYSQL_DB: mate
        MYSQL_USER: root
        MYSQL_ALLOW_EMPTY_PASSWORD: true
        MYSQL_PASSWORD:
    working_directory: /tmp/src/mydumper"
done

# COMMANDS


## SPECIFIC PREPARE COMMANDS

cat <<EOF

commands:

  prepare_mariadb1006:
    steps:
    - run: sudo bash /tmp/mariadb_repo_setup --skip-maxscale --mariadb-server-version "mariadb-10.6"
#    - run: yum-config-manager --disable mariadb-maxscale || true

  prepare_mariadb1011:
    steps:
    - run: sudo bash /tmp/mariadb_repo_setup --skip-maxscale --mariadb-server-version "mariadb-10.11"
#    - run: yum-config-manager --disable mariadb-maxscale || true

  prepare_el_mysql80:
    steps:
    - run: sudo yum install -y libasan gdb screen time mysql-community-libs mysql-community-devel mysql-community-client

  prepare_el_mysql84:
    steps:
    - run: sudo yum install -y libasan gdb screen time mysql-community-libs mysql-community-devel mysql-community-client

  prepare_apt_debian_default:
    steps:
    - run: sudo apt-get install -y default-mysql-client default-libmysqlclient-dev default-mysql-client-core

  prepare_apt_ubuntu_default:
    steps:
    - run: sudo apt-get install -y default-mysql-client default-libmysqlclient-dev default-mysql-client-core

  prepare_apt_percona57:
    steps:
    - run: sudo percona-release setup -y ps57
    - run: sudo apt-get install -y gdb screen time libperconaserverclient20 percona-server-client-5.7 libperconaserverclient20-dev

  prepare_apt_percona80:
    steps:
    - run: sudo percona-release setup -y ps80
    - run: sudo apt-get install -y gdb screen time libperconaserverclient21 libperconaserverclient21-dev percona-server-client

  prepare_apt_mysql84:
    steps:
    - run: echo "mysql-apt-config mysql-apt-config/select-product string Ok" | sudo debconf-set-selections
    - run: echo "mysql-apt-config mysql-apt-config/select-server string mysql-8.4-lts" | sudo debconf-set-selections
    - run: sudo rm /usr/share/keyrings/mysql-apt-config.gpg
    - run: echo "4" | DEBIAN_FRONTEND=noninteractive sudo dpkg-reconfigure mysql-apt-config
    - run: sudo apt-get update
    - run: sudo apt-get install -y gdb screen time libmysqlclient24 libmysqlclient-dev mysql-client

  prepare_apt_mariadb1006:
    steps:
    - run: sudo apt-get install -y gdb screen time mariadb-client libmariadbclient18 libmariadb-dev libmariadb-dev-compat || true
    - run: sudo apt-get install -y gdb screen time mariadb-client libmariadbclient18 libmariadb-dev libmariadb-dev-compat

  prepare_el_mariadb1006:
    steps:
    - prepare_mariadb1006
    - run: sudo yum install -y libasan gdb screen time MariaDB-devel
    - run: sudo yum install -y libasan gdb screen time MariaDB-compat || true

  prepare_apt_mariadb1011:
    steps:
    - run: sudo apt-get install -y gdb screen time mariadb-client libmariadbclient18 libmariadb-dev libmariadb-dev-compat || true
    - run: sudo apt-get install -y gdb screen time mariadb-client libmariadbclient18 libmariadb-dev libmariadb-dev-compat


  prepare_el7_mariadb1011:
    steps:
    - run: sudo bash /tmp/mariadb_repo_setup --skip-maxscale --mariadb-server-version "mariadb-10.11.11"
#    - run: yum-config-manager --disable mariadb-maxscale || true
    - run: sudo yum install -y libasan gdb screen time MariaDB-devel
    - run: sudo yum install -y libasan gdb screen time MariaDB-compat || true

  prepare_el7_mariadb1006:
    steps:
    - run: sudo bash /tmp/mariadb_repo_setup --skip-maxscale --mariadb-server-version "mariadb-10.6.21"
#    - run: yum-config-manager --disable mariadb-maxscale || true
    - run: sudo yum install -y libasan gdb screen time MariaDB-devel
    - run: sudo yum install -y libasan gdb screen time MariaDB-compat || true

  prepare_el_mariadb1011:
    steps:
    - prepare_mariadb1011
    - run: sudo yum install -y libasan gdb screen time MariaDB-devel
    - run: sudo yum install -y libasan gdb screen time MariaDB-compat || true
EOF

for os in el7 el9
do
    for vendor in ${list_mysql_version[@]} ${list_percona_version[@]}
    do
        echo "
  prepare_${all_os[${os}_0]}_${all_vendors[${vendor}_0]}:
    steps:
    - prepare_el_${all_vendors[${vendor}_0]}
"
    done
done

for vendor in ${list_mysql_version[@]} ${list_percona_version[@]}
do
  echo "
  prepare_el8_${all_vendors[${vendor}_0]}:
    steps:
    - run: sudo dnf module disable mysql -y
    - prepare_el_mysql80
"
done

for os in el8 el9
do
    for vendor in ${list_mariadb_version[@]}
    do
        echo "
  prepare_${all_os[${os}_0]}_${all_vendors[${vendor}_0]}:
    steps:
    - prepare_el_${all_vendors[${vendor}_0]}
"
    done
done



# On apt repositories OS the preparation
for os in ${list_ubuntu_os[@]} ${list_debian_os[@]}
do
    # For TIDB will be only with Percona57
    vendor=tidb
    echo "
  prepare_${all_os[${os}_0]}_${vendor}:
    steps:
    - run: sudo apt-get update
    - prepare_apt_percona57
"
    # For Percona and MySQL will be the standar apt preparation
    for vendor in ${list_percona_version[@]} ${list_mysql_version[@]}
    do
echo "
  prepare_${all_os[${os}_0]}_${all_vendors[${vendor}_0]}:
    steps:
    - run: sudo apt-get update
    - prepare_apt_${all_vendors[${vendor}_0]}
"
    done

    # For MariaDB we need some extra steps
    for vendor in ${list_mariadb_version[@]}
    do
echo "
  prepare_${all_os[${os}_0]}_${all_vendors[${vendor}_0]}:
    steps:
    - run: sudo apt-get update
    - prepare_${all_vendors[${vendor}_0]}
    - prepare_apt_${all_vendors[${vendor}_0]}
"
    done
done

cat <<EOF
  prepare_el_percona57:
    steps:
    - run: percona-release setup -y ps57
    - run: dnf -y module disable mysql || true
    - run: yum -y install libasan gdb screen time Percona-Server-devel-57 Percona-Server-client-57

  prepare_el_tidb:
    steps:
    - run: prepare_el_percona57

  prepare_el_percona80:
    steps:
    - run: percona-release setup -y ps80
    - run: yum -y install libasan gdb screen time percona-server-devel percona-server-client

  compile:
    parameters:
      CMAKED:
        default: ""
        type: string
    steps:
    - run:
        command: |
          source /etc/profile.d/sh.local || true
    - run: cmake . <<parameters.CMAKED>>
    - run: make VERBOSE=1
    - run: sudo make install
    - run: ./mydumper --version

  compile_and_test_mydumper:
    parameters:
      test:
        type: boolean
        default: true
    steps:
    - compile
    - when:
        condition: << parameters.test >>
        steps:
        - run: bash ./test_mydumper.sh SSL
    - store_artifacts:
        path: /tmp/stream.sql
        destination: artifact-file
    - store_artifacts:
        path: /tmp/data/

  set_env_vars:
    steps:
    - run:
        command: |
          if [ -z \${CIRCLE_TAG+x} ] ; then echo 'export CIRCLE_TAG="v0.11.1-1"' >> "\$BASH_ENV"; fi
          echo 'export MYDUMPER_VERSION=\$(  echo "\${CIRCLE_TAG:1}" | cut -d'-' -f1 ) ' >> "\$BASH_ENV"
          echo 'export MYDUMPER_REVISION=\$( echo "\${CIRCLE_TAG:1}" | cut -d'-' -f2 ) ' >> "\$BASH_ENV"
          cat /etc/profile.d/sh.local >> "\$BASH_ENV" || true
          cat \$BASH_ENV
          source "\$BASH_ENV"

jobs:

EOF

for os in ${list_ubuntu_os[@]} ${list_debian_os[@]}
do
    for vendor in ${list_all_vendors[@]} tidb
    do
        cat <<EOF
  compile_and_test_mydumper_in_${all_os[${os}_0]}_${all_vendors[${vendor}_0]}:
    parameters:
      test:
        type: boolean
        default: false
      e:
        type: string
        default: ${all_os[${os}_0]}_${all_vendors[${vendor}_0]}
    executor: << parameters.e >>
    resource_class: large
    steps:
    - checkout
    - prepare_${all_os[${os}_0]}_${all_vendors[${vendor}_0]}
    - compile_and_test_mydumper:
        test: << parameters.test >>
    - persist_to_workspace:
         root: /tmp/src/mydumper
         paths:
           - .

EOF
    done
done

for os in ${list_el_os[@]}
do
    for vendor in ${list_all_vendors[@]} tidb
    do
        cat <<EOF
  compile_and_test_mydumper_in_${all_os[${os}_0]}_${all_vendors[${vendor}_0]}:
    parameters:
      test:
        type: boolean
        default: false
      e:
        type: string
        default: ${all_os[${os}_0]}
    executor: << parameters.e >>
    resource_class: large
    steps:
    - checkout
    - prepare_${all_os[${os}_0]}_${all_vendors[${vendor}_0]}
    - compile_and_test_mydumper:
        test: << parameters.test >>
    - persist_to_workspace:
         root: /tmp/src/mydumper
         paths:
           - .

EOF
    done
done

for arch in ${list_arch[@]}
do
    for os in ${list_el_os[@]}
    do
        for vendor in ${list_all_vendors[@]}
        do
echo "  build_${all_os[${os}_0]}_${all_vendors[${vendor}_0]}_${all_arch[${arch}_rpm]}:
    executor: ${all_os[${os}_0]}
    resource_class: ${all_arch[${arch}_resource_class]}
    steps:
    - checkout"
if [ "${all_os[${os}_0]}_${all_vendors[${vendor}_0]}_${all_arch[${arch}_rpm]}" != "${build_man_os}" ]
then
echo '    - attach_workspace:
        at: /tmp/man'
fi
echo "    - set_env_vars
    - prepare_${os}_${all_vendors[${vendor}_0]}
    - run: mkdir -p /tmp/package
    - run: yum -y install rpmdevtools
    - compile:
        CMAKED: \"-DMYSQL_LIBRARIES_mysqlclient:FILEPATH=/usr/lib64/mysql/libmysqlclient.a\"
    - run: if (( \$(nm ./mydumper | grep -i mysql | grep \" T \" | wc -l) < 50 )); then false; fi
    - run: mkdir -p /tmp/src/mydumper/${all_os[${os}_0]}_${all_vendors[${vendor}_0]}_${all_arch[${arch}_rpm]}
    - run: cp /tmp/man/mydumper.1.gz /tmp/man/myloader.1.gz mydumper.cnf mydumper myloader /tmp/src/mydumper/${all_os[${os}_0]}_${all_vendors[${vendor}_0]}_${all_arch[${arch}_rpm]}/
    - run: ./package/build.sh \${MYDUMPER_VERSION} \${MYDUMPER_REVISION} rpm ${all_os[${os}_0]}_${all_vendors[${vendor}_0]}_${all_arch[${arch}_rpm]} ${all_arch[${arch}_rpm]}"
#        command: |
#          if [ -z ${CIRCLE_TAG+x} ];
#          then
#            export MYDUMPER_VERSION=\"9.9.9\"
#            export MYDUMPER_REVISION=\"9\"
#          fi
#          ./package/build.sh \${MYDUMPER_VERSION} \${MYDUMPER_REVISION} rpm ${all_os[${os}_0]}_${all_vendors[${vendor}_0]}_${all_arch[${arch}_rpm]} ${all_arch[${arch}_rpm]}"
echo "    - persist_to_workspace:
         root: /tmp/package
         paths:
           - ."
if [ "${all_os[${os}_0]}_${all_vendors[${vendor}_0]}_${all_arch[${arch}_rpm]}" = "${build_man_os}" ]
then
echo "    - persist_to_workspace:
         root: /tmp/man
         paths:
           - ."
fi
        done
    done
done

for arch in ${list_arch[@]}
do
    for os in ${list_ubuntu_os[@]} ${list_debian_os[@]}
    do
        for vendor in ${list_all_vendors[@]}
        do
echo "  build_${all_os[${os}_0]}_${all_vendors[${vendor}_0]}_${all_arch[${arch}_deb]}:
    executor: ${all_os[${os}_0]}
    resource_class: ${all_arch[${arch}_resource_class]}
    parameters:
      build_man:
        type: boolean
        default: false
    
    steps:
    - checkout"
if [ "${all_os[${os}_0]}_${all_vendors[${vendor}_0]}_${all_arch[${arch}_deb]}" != "${build_man_os}" ]
then
echo '    - attach_workspace:
        at: /tmp/man'
fi
echo "    - set_env_vars
    - prepare_${all_os[${os}_0]}_${all_vendors[${vendor}_0]}
    - run: sudo apt install -y fakeroot
    - run: mkdir -p /tmp/man/
    - compile:
        CMAKED: \"-DMYSQL_LIBRARIES_${all_vendors[${vendor}_3]}:FILEPATH=/usr/lib/${all_arch[${arch}_rpm]}-linux-gnu/lib${all_vendors[${vendor}_4]}.a\"
    - run: if (( \$(nm ./mydumper | grep -i mysql | grep \" T \" | wc -l) < 50 )); then false; fi
    - when:
        condition: << parameters.build_man >>
        steps:
        - run: git submodule update --init docs/
        - run: sudo apt-get install pip sphinx-common
        - run: pip install furo sphinx_copybutton sphinx-inline-tabs
        - run: 
            command: |
              cd docs
              cmake . -DWITH_HTML=OFF
              make
              gzip man/mydumper.1 man/myloader.1
              cp man/mydumper.1.gz  man/myloader.1.gz /tmp/man/
    - run: mkdir -p /tmp/src/mydumper/${all_os[${os}_0]}_${all_vendors[${vendor}_0]}_${all_arch[${arch}_deb]}/etc
    - run: cp /tmp/man/mydumper.1.gz /tmp/man/myloader.1.gz  mydumper.cnf mydumper myloader /tmp/src/mydumper/${all_os[${os}_0]}_${all_vendors[${vendor}_0]}_${all_arch[${arch}_deb]}/
    - run: ./package/build.sh \${MYDUMPER_VERSION} \${MYDUMPER_REVISION} deb ${all_os[${os}_0]}_${all_vendors[${vendor}_0]}_${all_arch[${arch}_deb]} ${all_arch[${arch}_deb]}"
#        command: |
#          if [ -z ${CIRCLE_TAG+x} ];
#          then
#            export MYDUMPER_VERSION=\"9.9.9\"
#            export MYDUMPER_REVISION=\"9\"
#          fi
#          ./package/build.sh \${MYDUMPER_VERSION} \${MYDUMPER_REVISION} deb ${all_os[${os}_0]}_${all_vendors[${vendor}_0]}_${all_arch[${arch}_deb]} ${all_arch[${arch}_deb]}"
echo '    - persist_to_workspace:
         root: /tmp/package
         paths:
           - .'
if [ "${all_os[${os}_0]}_${all_vendors[${vendor}_0]}_${all_arch[${arch}_deb]}" = "${build_man_os}" ]
then
echo '    - persist_to_workspace:
         root: /tmp/man
         paths:
           - .'
fi
        done
    done
done

echo '
  publish-github-release:
    docker:
      - image: cibuilds/github:0.10
    steps:
    - attach_workspace:
        at: /tmp/package
    - run:
        command: |
          rm /tmp/package/mydumper.1.gz /tmp/package/myloader.1.gz
          ghr -t ${GITHUB_TOKEN} -u ${CIRCLE_PROJECT_USERNAME} -r ${CIRCLE_PROJECT_REPONAME} -c ${CIRCLE_SHA1} -b "$( cd /tmp/package/; echo -e "MD5s:\n\`\`\`"; md5sum * ;echo -e "\`\`\`\nSHA1s:\n\`\`\`"; sha1sum * ; echo -e "\`\`\`\nSHA256s:\n\`\`\`"; sha256sum * ;echo -e "\`\`\`\n" )" -prerelease -draft -delete ${CIRCLE_TAG} /tmp/package'

echo -n '
  publish-repository:
    docker:
      - image: mydumper/mydumper-builder-noble
    steps:
    - run: sudo apt install -y git dpkg-dev apt-utils createrepo-c rpm reprepro
    - attach_workspace:
        at: /tmp/package    
    - run: echo ${MYDUMPER_REPO_PK} | base64 -d | gpg --import
    - run: git clone --no-checkout --filter=tree:0 https://github.com/mydumper/mydumper_repo.git mydumper_repo
    - run:
        command: |
          cd mydumper_repo/
          export DIR_SUFFIX="." && [ $(($(echo "${CIRCLE_TAG}" | cut -d'.' -f3 | cut -d'-' -f1)%2)) -eq 0 ] && export DIR_SUFFIX="testing"
          export APT_REPO="main" && [ $(($(echo "${CIRCLE_TAG}" | cut -d'.' -f3 | cut -d'-' -f1)%2)) -eq 0 ] && export APT_REPO="testing"
          export REPREPRO_OPTIONS="" && [ $(($(echo "${CIRCLE_TAG}" | cut -d'.' -f3 | cut -d'-' -f1)%2)) -eq 0 ] && export REPREPRO_OPTIONS=" -C testing "
          export BASE_PATH=$(pwd)
          export UBUNTU_PATH="apt/ubuntu"
          export DEBIAN_PATH="apt/debian"
          export YUM_PATH="yum"
          git config --global user.name "David Ducos"
          git config --global user.email "david.ducos@gmail.com"
          git checkout HEAD~ yum/rpmmacros
          cp yum/rpmmacros ~/.rpmmacros
          git remote set-url origin https://x-access-token:${GITHUB_TOKEN}@github.com/mydumper/mydumper_repo.git
          git reset HEAD
          mkdir -p $UBUNTU_PATH $DEBIAN_PATH $YUM_PATH'
	  
echo -n '
          cd ${BASE_PATH}
          cd ${UBUNTU_PATH}
          git checkout conf dists db'
for i in ${list_ubuntu_os[@]} ; do
echo -n '
          reprepro ${REPREPRO_OPTIONS}'" includedeb ${i} /tmp/package/mydumper*${i}*deb"
done
echo -n '
          git add $(find -type f)
          cd ${BASE_PATH}
          cd ${DEBIAN_PATH}
          git checkout conf dists db'
for i in ${list_debian_os[@]} ; 
do
echo -n ' 
          reprepro ${REPREPRO_OPTIONS}'" includedeb ${i} /tmp/package/mydumper*${i}*deb"
done
echo -n '
          git add $(find -type f)
          cd ${BASE_PATH}
          cd ${YUM_PATH}'
echo -n '
          gpg --export -a 79EA15C0E82E34BA > key.asc
          rpm --import key.asc
          rpm -q gpg-pubkey --qf "%{name}-%{version}-%{release} --> %{summary}\n"'

for i in ${list_el_os_without_el[@]} ; do
echo -n "
          mkdir -p ${i}/"
echo -n '${DIR_SUFFIX}/'
echo -n '
          cp '
echo -n "/tmp/package/mydumper*el${i}*rpm ${i}/"
echo -n '${DIR_SUFFIX}/'
echo -n "
          cd ${i}/"
echo -n '${DIR_SUFFIX}/
          rpm --addsign  *.rpm
          git add *.rpm'
echo -n '
          createrepo_c --update . 
          git add repodata
          cd ${BASE_PATH}
          cd ${YUM_PATH}'
done

echo '
          cd ${BASE_PATH}
          git commit -m "Upload repo files ${CIRCLE_TAG}" && git push'

echo '
    - run: 
        command: |
          git clone https://github.com/mydumper/mydumper.git mydumper
          cd mydumper
          git remote set-url origin https://x-access-token:${GITHUB_TOKEN}@github.com/mydumper/mydumper.git
          curl https://github.com/mydumper/mydumper_repo/info/refs?service=git-upload-pack --output ../latest_commit
          git update-index --cacheinfo 160000,$(head -2 ../latest_commit | tail -1 | cut -b9-48),repo
          git commit -am "[skip ci] Auto updated submodule references" && git push

  update_repo:
    docker:
      - image: mydumper/mydumper-builder-noble
    steps:
    - run:
        command: |
          git config --global user.name "David Ducos"
          git config --global user.email "david.ducos@gmail.com"
          git clone https://github.com/mydumper/mydumper.git mydumper
          cd mydumper
          git remote set-url origin https://x-access-token:${GITHUB_TOKEN}@github.com/mydumper/mydumper.git
          curl https://github.com/mydumper/mydumper_repo/info/refs?service=git-upload-pack --output ../latest_commit
          git update-index --cacheinfo 160000,$(head -2 ../latest_commit | tail -1 | cut -b9-48),repo
          git commit -am "[skip ci] Auto updated submodule references" && git push



parameters:
  my_trigger_parameter:
    type: string
    default: ""


workflows:
  version: 2

  api-update-repo:
    when: << pipeline.parameters.my_trigger_parameter >>
    jobs:
    - update_repo

#  api-update-repo-commit:
#    when: 
#      not: << pipeline.parameters.my_trigger_parameter >>
#    jobs:
#    - update_repo

  mydumper:
    jobs:'

for lc in ${!list_compile[@]}
do
echo "    - compile_and_test_mydumper_in_${list_compile[${lc}]}"
# Decomment next 5 lines if you want to ignore compilation and add : to previous line
#echo '        filters:
#          branches:
#            ignore: /.*/
#          tags:
#            only: /^v\d+\.\d+\.\d+-\d+$/'
done

for lt in ${!list_test[@]}
do
echo "    - compile_and_test_mydumper_in_${list_test[${lt}]}:
        test: true
        e: ${list_test[${lt}]}"
# Decomment next 5 lines if you want to ignore compilation
#echo '        filters:
#          branches:
#            ignore: /.*/
#          tags:
#            only: /^v\d+\.\d+\.\d+-\d+$/'

done

for os in ${list_build[@]}
do
echo "    - build_${os}:"
if [ "${os}" = "${build_man_os}" ]
then
echo "        build_man: true"
else
echo "        requires:
          - build_${build_man_os}"
fi

# Comment next 5 lines if you want to build
echo '        filters:
          branches:
            ignore: /.*/
          tags:
            only: /^v\d+\.\d+\.\d+-\d+$/'
done

echo '
    - publish-github-release:
        requires:'
for os in ${!list_build[@]}
do
echo "          - build_${list_build[${os}]}"
done
echo '        filters:
          branches:
            ignore: /.*/
          tags:
#            only: /.*/
            only: /^v\d+\.\d+\.\d+-\d+$/

    - publish-repository:
        requires:
          - publish-github-release
        filters:
          branches:
            ignore: /.*/
          tags:
#            only: /.*/
            only: /^v\d+\.\d+\.\d+-\d+$/
'
