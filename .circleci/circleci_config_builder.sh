#!/bin/bash

echo "---
version: 2.1

orbs:
  capture-tag: rvla/capture-tag@0.0.2

executors:"

# TODO: make ASAN counterpart of builders

declare -A all_vendors
vendor=mysql80
all_vendors[${vendor}_0]="mysql80"
all_vendors[${vendor}_1]="mysql:8"
vendor=percona57
all_vendors[${vendor}_0]="percona57"
all_vendors[${vendor}_1]="percona:5.7"
vendor=percona80
all_vendors[${vendor}_0]="percona80"
all_vendors[${vendor}_1]="percona:8"

vendor=mariadb1004
all_vendors[${vendor}_0]="mariadb1004"
all_vendors[${vendor}_1]="mariadb:10.04-rc"
all_vendors[${vendor}_2]="mariadb-10.04"

vendor=mariadb1005
all_vendors[${vendor}_0]="mariadb1005"
all_vendors[${vendor}_1]="mariadb:10.05-rc"
all_vendors[${vendor}_2]="mariadb-10.05"

vendor=mariadb1006
all_vendors[${vendor}_0]="mariadb1006"
all_vendors[${vendor}_1]="mariadb:10.6"
all_vendors[${vendor}_2]="mariadb-10.6"

vendor=mariadb1011
all_vendors[${vendor}_0]="mariadb1011"
all_vendors[${vendor}_1]="mariadb:10.11-rc"
all_vendors[${vendor}_2]="mariadb-10.11"

vendor=tidb
all_vendors[${vendor}_0]="tidb"
all_vendors[${vendor}_1]="pingcap/tidb"



#list_all_vendors=( "percona57" "percona80" "mariadb1004" "mariadb1005" "mariadb1006" "mariadb1011")
list_all_vendors=( "percona57" "percona80" "mariadb1011" "mariadb1006")
list_mysql_version=( "mysql80" )
list_percona_version=( "percona57" "percona80" )
list_mariadb_version=( "mariadb1004" "mariadb1005" "mariadb1006" "mariadb1011" )
list_mariadb_version=( "mariadb1011" "mariadb1006")
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

os=buster
all_os[${os}_0]="buster"
all_os[${os}_1]="debian:buster"
all_os[${os}_2]="percona-release_latest.buster_all.deb"
all_os[${os}_3]="false"

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


# os=
# all_os[${os}_0]=""
# all_os[${os}_1]=""
list_el_os=("el7" "el8" "el9")
list_ubuntu_os=("bionic" "focal" "jammy")
list_debian_os=("buster" "bullseye" "bookworm")
list_all_os=("bionic" "focal" "jammy" "el7" "el8" "el9" "buster" "bullseye" "bookworm")


list_build=("bionic_percona80_arm64" "bionic_percona80_amd64" "focal_percona80_arm64" "focal_percona80_amd64" "jammy_percona80_amd64" "jammy_percona80_arm64" "el7_percona57_aarch64" "el7_percona57_x86_64" "el8_percona57_aarch64" "el8_percona57_x86_64" "el9_percona80_aarch64" "el9_percona80_x86_64" "bullseye_percona80_amd64" "bullseye_percona80_arm64" "buster_percona80_arm64" "buster_percona80_amd64")

#list_build=("bionic_percona80_amd64" "focal_percona80_amd64" "jammy_percona80_amd64" "el7_percona57_x86_64" "el8_percona57_x86_64" "el8_mysql80_aarch64"  "el9_mysql80_aarch64" "el9_percona80_x86_64" "bullseye_percona80_amd64" "buster_percona80_amd64" "bookworm_percona80_amd64")

#list_build=("bionic_percona80_amd64" "focal_percona80_amd64" "jammy_percona80_amd64" "el7_percona57_x86_64" "el8_percona57_x86_64" "el9_percona80_x86_64" "bullseye_percona80_amd64" "buster_percona80_amd64")

filter_out="jammy|el9_percona57|el7_mysql80_aarch64|bookworm_mariadb1006|bionic_mariadb1011|bionic_mariadb1006"

for os in ${list_all_os[@]}
do
    for vendor in ${list_all_vendors[@]} tidb
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
        MYSQL_USER: root
        MYSQL_ALLOW_EMPTY_PASSWORD: true
    working_directory: /tmp/src/mydumper"
    done
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

cat <<EOF

commands:

  prepare_mariadb1006:
    steps:
    - run: sudo bash /tmp/mariadb_repo_setup --mariadb-server-version "mariadb-10.6"

  prepare_mariadb1011:
    steps:
    - run: sudo bash /tmp/mariadb_repo_setup --mariadb-server-version "mariadb-10.11"

  prepare_el_mysql80:
    steps:
    - run: sudo yum install -y libasan gdb screen time mysql-community-libs mysql-community-devel mysql-community-client

  prepare_el8_mysql80:
    steps:
    - run: sudo dnf module disable mysql -y
    - prepare_el_mysql80

  prepare_ubuntu_percona57:
    steps:
    - run: sudo percona-release setup -y ps57
    - run: sudo apt-get install -y gdb screen time libperconaserverclient20 percona-server-client-5.7 libperconaserverclient20-dev

  prepare_ubuntu_percona80:
    steps:
    - run: sudo percona-release setup -y ps80
    - run: sudo apt-get install -y gdb screen time libperconaserverclient21 libperconaserverclient21-dev percona-server-client


  prepare_ubuntu_mariadb1006:
    steps:
    - run: sudo apt-get install -y gdb screen time mariadb-client libmariadbclient18 libmariadb-dev libmariadb-dev-compat || true
    - run: sudo apt-get install -y gdb screen time mariadb-client libmariadbclient18 libmariadb-dev libmariadb-dev-compat

  prepare_el_mariadb1006:
    steps:
    - prepare_mariadb1006
    - run: sudo yum install -y libasan gdb screen time MariaDB-devel
    - run: sudo yum install -y libasan gdb screen time MariaDB-compat || true

  prepare_ubuntu_mariadb1011:
    steps:
    - run: sudo apt-get install -y gdb screen time mariadb-client libmariadbclient18 libmariadb-dev libmariadb-dev-compat || true
    - run: sudo apt-get install -y gdb screen time mariadb-client libmariadbclient18 libmariadb-dev libmariadb-dev-compat

  prepare_el_mariadb1011:
    steps:
    - prepare_mariadb1011
    - run: sudo yum install -y libasan gdb screen time MariaDB-devel
    - run: sudo yum install -y libasan gdb screen time MariaDB-compat || true
EOF

for os in el7 el9
do
    for vendor in ${list_mysql_version[@]}
    do
        echo "
  prepare_${all_os[${os}_0]}_${all_vendors[${vendor}_0]}:
    steps:
    - prepare_el_${all_vendors[${vendor}_0]}
"
    done
done



for os in ${list_el_os[@]}
do
    for vendor in ${list_mariadb_version[@]} ${list_percona_version[@]}
    do
        echo "
  prepare_${all_os[${os}_0]}_${all_vendors[${vendor}_0]}:
    steps:
    - prepare_el_${all_vendors[${vendor}_0]}
"
    done
done


for os in ${list_ubuntu_os[@]} ${list_debian_os[@]}
do
    vendor=tidb
    echo "
  prepare_${all_os[${os}_0]}_${vendor}:
    steps:
    - prepare_ubuntu_percona57
"

    for vendor in ${list_percona_version[@]}
    do
echo "
  prepare_${all_os[${os}_0]}_${all_vendors[${vendor}_0]}:
    steps:
    - prepare_ubuntu_${all_vendors[${vendor}_0]}
"
    done

    for vendor in ${list_mariadb_version[@]}
    do
echo "
  prepare_${all_os[${os}_0]}_${all_vendors[${vendor}_0]}:
    steps:
    - prepare_${all_vendors[${vendor}_0]}
    - prepare_ubuntu_${all_vendors[${vendor}_0]}
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
          echo 'export MYDUMPER_VERSION=\$(  echo "\${CIRCLE_TAG:1}" | cut -d'-' -f1 ) ' >> "\$BASH_ENV"
          echo 'export MYDUMPER_REVISION=\$( echo "\${CIRCLE_TAG:1}" | cut -d'-' -f2 ) ' >> "\$BASH_ENV"
          cat /etc/profile.d/sh.local >> "\$BASH_ENV" || true
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
#    - prepare_ubuntu
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
#    - prepare_el
    - prepare_el_${all_vendors[${vendor}_0]}
    - compile_and_test_mydumper:
        test: << parameters.test >>
    - persist_to_workspace:
         root: /tmp/src/mydumper
         paths:
           - .

EOF
    done

    for vendor in ${list_mysql_version[@]}
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
#    - prepare_el
    - prepare_${os}_${all_vendors[${vendor}_0]}
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
        for vendor in ${list_all_vendors[@]} ${list_mysql_version[@]}
        do
echo "  build_${all_os[${os}_0]}_${all_vendors[${vendor}_0]}_${all_arch[${arch}_rpm]}:
#    executor: ${all_os[${os}_0]}_${all_vendors[${vendor}_0]}
    executor: ${all_os[${os}_0]}
    resource_class: ${all_arch[${arch}_resource_class]}
    steps:
    - checkout
    - set_env_vars
#    - prepare_el
    - prepare_${os}_${all_vendors[${vendor}_0]}
    - run: mkdir -p /tmp/package
    - run: yum -y install rpmdevtools
    - compile:
        CMAKED: \"-DMYSQL_LIBRARIES_mysqlclient:FILEPATH=/usr/lib64/mysql/libmysqlclient.a\"
    - run: if (( \$(nm ./mydumper | grep -i mysql | grep \" T \" | wc -l) < 50 )); then false; fi
    - run: mkdir -p /tmp/src/mydumper/${all_os[${os}_0]}_${all_vendors[${vendor}_0]}_${all_arch[${arch}_rpm]}
    - run: cp mydumper.cnf mydumper myloader /tmp/src/mydumper/${all_os[${os}_0]}_${all_vendors[${vendor}_0]}_${all_arch[${arch}_rpm]}/
    - run: ./package/build.sh \${MYDUMPER_VERSION} \${MYDUMPER_REVISION} rpm ${all_os[${os}_0]}_${all_vendors[${vendor}_0]}_${all_arch[${arch}_rpm]} ${all_arch[${arch}_rpm]}

    - persist_to_workspace:
         root: /tmp/package
         paths:
           - ."
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
#    executor: ${all_os[${os}_0]}_${all_vendors[${vendor}_0]}
    executor: ${all_os[${os}_0]}
    resource_class: ${all_arch[${arch}_resource_class]}
    steps:
    - checkout
    - set_env_vars
#    - prepare_ubuntu
    - prepare_${all_os[${os}_0]}_${all_vendors[${vendor}_0]}
    - run: sudo apt install -y fakeroot
    - run: mkdir -p /tmp/package
    - compile:
        CMAKED: \"-DMYSQL_LIBRARIES_perconaserverclient:FILEPATH=/usr/lib/x86_64-linux-gnu/libperconaserverclient.a\"
    - run: if (( \$(nm ./mydumper | grep -i mysql | grep \" T \" | wc -l) < 50 )); then false; fi
    - run: mkdir -p /tmp/src/mydumper/${all_os[${os}_0]}_${all_vendors[${vendor}_0]}_${all_arch[${arch}_deb]}/etc
    - run: cp mydumper.cnf mydumper myloader /tmp/src/mydumper/${all_os[${os}_0]}_${all_vendors[${vendor}_0]}_${all_arch[${arch}_deb]}/
    - run: ./package/build.sh \${MYDUMPER_VERSION} \${MYDUMPER_REVISION} deb ${all_os[${os}_0]}_${all_vendors[${vendor}_0]}_${all_arch[${arch}_deb]} ${all_arch[${arch}_deb]}

    - persist_to_workspace:
         root: /tmp/package
         paths:
           - ."
        done
    done
done

echo '  publish-github-release:
    docker:
      - image: cibuilds/github:0.10
    steps:
    - attach_workspace:
        at: /tmp/package
    - run: ghr -t ${GITHUB_TOKEN} -u ${CIRCLE_PROJECT_USERNAME} -r ${CIRCLE_PROJECT_REPONAME} -c ${CIRCLE_SHA1} -prerelease -draft -delete ${CIRCLE_TAG} /tmp/package

workflows:
  version: 2
  mydumper:
    jobs:'

for os in ${list_all_os[@]}
do
    for vendor in ${list_all_vendors[@]}
    do
echo "    - compile_and_test_mydumper_in_${all_os[${os}_0]}_${all_vendors[${vendor}_0]}" | egrep -v "${filter_out}"
    done
done

for os in ${list_el_os[@]}
do
    for vendor in ${list_mysql_version[@]}
    do
echo "    - compile_and_test_mydumper_in_${all_os[${os}_0]}_${all_vendors[${vendor}_0]}" | egrep -v "${filter_out}"
    done
done


for os in jammy
do
    for vendor in ${list_all_vendors[@]} # tidb
    do
echo "    - compile_and_test_mydumper_in_${all_os[${os}_0]}_${all_vendors[${vendor}_0]}:
        test: true
        e: ${all_os[${os}_0]}_${all_vendors[${vendor}_0]}
"
    done
done

for os in ${list_build[@]}
do
echo "    - build_${os}:"
echo '        filters:
          branches:
            ignore: /.*/
          tags:
            only: /^v\d+\.\d+\.\d+-\d+$/'
done

echo '    - publish-github-release:
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
            only: /^v\d+\.\d+\.\d+-\d+$/'
