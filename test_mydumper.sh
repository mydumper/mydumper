#!/bin/bash
mydumper_log="/tmp/test_mydumper.log"
tmp_mydumper_log="/tmp/test_mydumper.log.tmp"
myloader_log="/tmp/test_myloader.log"
tmp_myloader_log="/tmp/test_myloader.log.tmp"
mydumper_stor_dir="/tmp/data"
mysqldumplog=/tmp/mysqldump.sql
myloader_stor_dir=$mydumper_stor_dir
stream_stor_dir="/tmp/stream_data"

die()
{
    [ -n "$1" ] && echo "$1" >&2;
    exit 1
}

finish()
{
  echo "Test finished successfully!"
  exit 0
}

if [ -x ./mydumper -a -x ./myloader ]
then
  mydumper="./mydumper"
  myloader="./myloader"
else
  mydumper=`which mydumper` ||
    die "mydumper not found!"
  myloader=`which myloader` ||
    die "myloader not found!"
fi

mysqldump_exe=`which mariadb-dump` ||
mysqldump_exe=`which mysqldump` ||
  die "mysqldump client not found!"

mysql_exe=`which mariadb` ||
mysql_exe=`which mysql` ||
  die "mysql client not found!"

if [ -z "$MYSQLX_UNIX_PORT" -a -z "$MYSQL_UNIX_PORT" ]
then
  for d in /var/lib/mysql /var/run /tmp
  do
    if [ -S $d/mysqlx.sock ]; then
      export MYSQLX_UNIX_PORT=$d/mysqlx.sock
      echo "X socket: $MYSQLX_UNIX_PORT"
    fi
    if [ -S $d/mysql.sock ]; then
      export MYSQL_UNIX_PORT=$d/mysql.sock
      echo "Socket: $MYSQL_UNIX_PORT"
      break
    elif [ -S $d/mysqld.sock ]; then
      export MYSQL_UNIX_PORT=$d/mysqld.sock
      echo "Socket: $MYSQL_UNIX_PORT"
      break
    fi
  done
fi

if [ -z "$MYSQLX_UNIX_PORT" -a -z "$MYSQL_UNIX_PORT" ]
then
  export MYSQL_HOST=127.0.0.1
  export MYSQL_TCP_PORT=3306
  echo "Using TCP connection to $MYSQL_HOST:$MYSQL_TCP_PORT"
fi

mysqldump()
{
  # mysqldump doesn't respect $MYSQL_HOST
  local host_arg=${MYSQL_HOST:+-h $MYSQL_HOST}
  $mysqldump_exe --no-defaults $host_arg -f -uroot "$@" || exit
}
export -f mysqldump

mysql()
{
  # mysql seems to respect $MYSQL_HOST
  $mysql_exe --no-defaults -f -uroot "$@" || exit
}
export -f mysql

# LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libjemalloc.so
# export LD_PRELOAD
export G_DEBUG=fatal-criticals
> $mydumper_log
> $myloader_log

optstring_long="case:"
optstring_short="c:"

opts=$(getopt -o "${optstring_short}" --long "${optstring_long}" --name "$0" -- "$@") ||
    exit $?
eval set -- "$opts"

unset case_num
unset case_repeat

while true
do
  case "$1" in
  -c|--case)
    if [[ "$2" == *:* ]]
    then
      case_repeat=${2##*:}
      [[ case_repeat -lt 1 ]] &&
        case_repeat=$(printf "%u/2\n" -2 | bc) # infinity (almost)
    fi
    case_num=${2%%:*}
    case_num=${case_num###}
    echo "Executing test case: #${case_num}${case_repeat:+ for $case_repeat times}  "
    case_repeat=${case_repeat:-1}
    shift 2;;
  --) shift; break;;
  esac
done


for i in $*
do
  if [ "$($mydumper --version | grep "$i" | wc -l)" != "1" ]
  then
    exit 1
  fi
  if [ "$($myloader --version | grep "$i" | wc -l)" != "1" ]
  then
    exit 1
  fi
done

if [ -x /usr/bin/time ]
then
  declare -a time2=(/usr/bin/time -f 'real: %e; usr: %U; sys: %S; data: %D; faults: %F; rfaults: %R; fsi: %I; fso: %O; socki: %r; socko: %s; mem: %K; rss_avg: %t; rss_max: %M; shared: %X; stack: %p; cpu: %P; swaps: %W; ctx0: %c; ctx1: %w; sigs: %k; ret: %x')
else
  declare -a time2=(time -p)
fi

ulimit -c unlimited
core_pattern=$(cat /proc/sys/kernel/core_pattern)
echo "Core pattern: $core_pattern"
echo "Core limit: $(ulimit -c)"

print_core()
{
  [[ -n "$(which gdb 2> /dev/null)" && "$core_pattern" =~ ^core ]] ||
    return

  local core=$(find . -name "core*" -print -quit)
  if [ -n "$core" ]
  then
    gdb -q --batch -c $core $mydumper -ex "set print frame-arguments all" -ex "bt full"
    rm -f "$core"
  fi
}

backtrace ()
{
   echo "Backtrace is:"
   local i=0
   while caller $i
   do
      i=$((i+1))
   done
}

test_case_dir (){
  # Test case
  # We should consider each test case, with different mydumper/myloader parameters
  s=$*

  echo "Case #${number}${case_cycle:+:$case_cycle}"

  mydumper_parameters=${s%%"-- "*}
  myloader_parameters=${s#*"-- "}

  if [ "${mydumper_parameters}" != "" ]
  then
    # Prepare
    rm -rf ${mydumper_stor_dir}
    mkdir -p ${mydumper_stor_dir}
    # Export
    echo "Exporting database: ${mydumper_parameters}"
    rm -rf /tmp/fifodir
    "${time2[@]}" $mydumper -u root -M -v 4 -L $tmp_mydumper_log ${mydumper_parameters}
    error=$?
    cat $tmp_mydumper_log >> $mydumper_log
    if (( $error > 0 ))
    then
      print_core
      echo "Retrying export due error"
      echo "Exporting database: ${mydumper_parameters}"
      rm -rf /tmp/fifodir
      rm -rf ${mydumper_stor_dir} ${myloader_stor_dir}
      $mydumper -u root -M -v 4 -L $tmp_mydumper_log ${mydumper_parameters}
      error=$?
      cat $tmp_mydumper_log >> $mydumper_log
      if (( $error > 0 ))
        then
        print_core
        mysqldump --all-databases > $mysqldumplog
        echo "Error running: $mydumper -u root -M -v 4 -L $mydumper_log ${mydumper_parameters}"
        cat $tmp_mydumper_log
        mv $tmp_mydumper_log $mydumper_stor_dir
        backtrace
        exit $error
      fi
    fi
  fi
  if [ "$PARTIAL" != "1" ]
  then
  echo "DROP DATABASE IF EXISTS sakila;
DROP DATABASE IF EXISTS myd_test;
DROP DATABASE IF EXISTS myd_test_no_fk;
DROP DATABASE IF EXISTS empty_db;" | mysql
  fi
  if [ "${myloader_parameters}" != "" ]
  then
    # Import
    echo "Importing database: ${myloader_parameters}"
    mysqldump --all-databases > $mysqldumplog
    rm -rf /tmp/fifodir
    "${time2[@]}" $myloader -u root -v 4 -L $tmp_myloader_log ${myloader_parameters}
    error=$?
    cat $tmp_myloader_log >> $myloader_log
    if (( $error > 0 ))
    then
      print_core
      echo "Retrying import due error"
      echo "Importing database: ${myloader_parameters}"
      mysqldump --all-databases > $mysqldumplog
      rm -rf /tmp/fifodir
      $myloader -u root -v 4 -L $tmp_myloader_log ${myloader_parameters}
      error=$?
      cat $tmp_myloader_log >> $myloader_log
      if (( $error > 0 ))
      then
        print_core
        mv $mysqldumplog $mydumper_stor_dir
        echo "Error running: $myloader -u root -v 4 -L $myloader_log ${myloader_parameters}"
        echo "Error running myloader with mydumper: $mydumper -u root -M -v 4 -L $mydumper_log ${mydumper_parameters}"
        cat $tmp_mydumper_log
        cat $tmp_myloader_log
        mv $tmp_mydumper_log $mydumper_stor_dir
        mv $tmp_myloader_log $mydumper_stor_dir
        backtrace
        exit $error
      fi
    fi
  fi
}


test_case_stream (){
  # Test case
  # We should consider each test case, with different mydumper/myloader parameters
  s=$*

  number=$(( $number + 1 ))

  echo "Case #${number}${case_cycle:+:$case_cycle}"

  mydumper_parameters=${s%%"-- "*}
  myloader_parameters=${s#*"-- "}

  if [ "${mydumper_parameters}" != "" ] && [ "${myloader_parameters}" != "" ]
  then
    # Prepare
    rm -rf ${mydumper_stor_dir} ${myloader_stor_dir}
    mkdir -p ${mydumper_stor_dir} ${myloader_stor_dir}
    # Export
    echo "Exporting database: $mydumper --stream -u root -M -v 4 -L $tmp_mydumper_log ${mydumper_parameters} | $myloader  ${myloader_general_options} -u root -v 4 -L $tmp_myloader_log ${myloader_parameters} --stream"
    rm -rf /tmp/fifodir
    "${time2[@]}" $mydumper --stream -u root -M -v 4 -L $tmp_mydumper_log ${mydumper_parameters} > /tmp/stream.sql
    error=$?
    mysqldump --all-databases > $mysqldumplog
    if (( $error > 0 ))
    then
      echo "Retrying export due error"
      echo "Exporting database: $mydumper --stream -u root -M -v 4 -L $tmp_mydumper_log ${mydumper_parameters} | $myloader  ${myloader_general_options} -u root -v 4 -L $tmp_myloader_log ${myloader_parameters} --stream"
      rm -rf ${mydumper_stor_dir} ${myloader_stor_dir}
      rm -rf /tmp/fifodir
      $mydumper --stream -u root -M -v 4 -L $tmp_mydumper_log ${mydumper_parameters} > /tmp/stream.sql
      error=$?
      mysqldump --all-databases > $mysqldumplog
      if (( $error > 0 ))
      then
        echo "Error running: $mydumper --stream -u root -M -v 4 -L $mydumper_log ${mydumper_parameters}"
        cat $tmp_mydumper_log
        mv $tmp_mydumper_log $mydumper_stor_dir
        backtrace
        exit $error
      fi
    fi
  if [ "$PARTIAL" != "1" ]
  then
  echo "DROP DATABASE IF EXISTS sakila;
DROP DATABASE IF EXISTS myd_test;
DROP DATABASE IF EXISTS myd_test_no_fk;
DROP DATABASE IF EXISTS empty_db;" | mysql
  fi
    rm -rf /tmp/fifodir
    cat /tmp/stream.sql | $myloader ${myloader_general_options} -u root -v 4 -L $tmp_myloader_log ${myloader_parameters} --stream
    error=$?
    cat $tmp_myloader_log >> $myloader_log
    cat $tmp_mydumper_log >> $mydumper_log
    if (( $error > 0 ))
    then
      echo "Retrying import due error"
      rm -rf /tmp/fifodir
      cat /tmp/stream.sql | $myloader ${myloader_general_options} -u root -v 4 -L $tmp_myloader_log ${myloader_parameters} --stream
      error=$?
      cat $tmp_myloader_log >> $myloader_log
      cat $tmp_mydumper_log >> $mydumper_log
      if (( $error > 0 ))
      then
        mv $mysqldumplog $mydumper_stor_dir
        echo "Error running: $mydumper --stream -u root -M -v 4 -L $mydumper_log ${mydumper_parameters}"
        echo "Error running: $myloader ${myloader_general_options} -u root -v 4 -L $myloader_log ${myloader_parameters} --stream"
        cat $tmp_mydumper_log
        cat $tmp_myloader_log
        mv $tmp_mydumper_log $mydumper_stor_dir
        mv $tmp_myloader_log $mydumper_stor_dir
        backtrace
        exit $error
      fi
    fi
  fi
}

do_case()
{
  number=$(( $number + 1 ))
  if [[ -n "$case_num"  ]]
  then
    if [[ "$case_num" -ne $number ]]
    then
      return
    fi
    case_cycle=0
    while ((case_cycle++ < case_repeat))
    do
      "$@" || exit
    done
    finish
  fi
  unset case_cycle
  "$@"
}

number=0

prepare_full_test()
{
  if [ ! -f "sakila-db.tar.gz" ]; then
    wget -O sakila-db.tar.gz https://midenok-forks.github.io/sakila-db.tar.gz ||
      exit
  fi
  tar xzf sakila-db.tar.gz
  sed -i 's/last_update TIMESTAMP/last_update TIMESTAMP NOT NULL/g;s/NOT NULL NOT NULL/NOT NULL/g' sakila-db/sakila-schema.sql
  mysql < sakila-db/sakila-schema.sql
  mysql < sakila-db/sakila-data.sql

  echo "Import testing database"
  DATABASE=myd_test
  mysql < test/mydumper_testing_db.sql

  # export -- import
  # 1000 rows -- database must not exist

  mydumper_general_options="-u root -R -E -G -o ${mydumper_stor_dir} --regex '^(?!(mysql\.|sys\.))' --fifodir=/tmp/fifodir $MYDUMPER_ARGS"
  myloader_general_options="-o --max-threads-for-index-creation=1 --max-threads-for-post-actions=1  --fifodir=/tmp/fifodir $MYLOADER_ARGS"
}

full_test_global(){
  prepare_full_test
  # single file compressed -- overriting database
#  test_case_dir -c ${mydumper_general_options}                                 -- ${myloader_general_options} -d ${myloader_stor_dir}
  PARTIAL=0
  for test in test_case_dir test_case_stream
  do 
    echo "Executing test: $test"
    for compress_mode in "" "-c GZIP" "-c ZSTD"
      do
      for backup_mode in "" "--load-data" "--csv"
        do
        for innodb_optimize_key_mode in "" "--innodb-optimize-keys=AFTER_IMPORT_ALL_TABLES" "--innodb-optimize-keys=AFTER_IMPORT_PER_TABLE" 
          do
          for rows_and_filesize_mode in "" "-r 1000" "-r 10:100:10000" "-F 10" "-r 10:100:10000 -F 10" 
            do
            do_case $test $backup_mode $compress_mode $rows_and_filesize_mode                                 ${mydumper_general_options} -- ${myloader_general_options} -d ${myloader_stor_dir} --serialized-table-creation $innodb_optimize_key_mode
            # statement size to 2MB -- overriting database
            do_case $test $backup_mode $compress_mode $rows_and_filesize_mode -s 2000000                      ${mydumper_general_options} -- ${myloader_general_options} -d ${myloader_stor_dir} --serialized-table-creation $innodb_optimize_key_mode
            # compress and rows
            $test $backup_mode $compress_mode $rows_and_filesize_mode --use-savepoints --less-locking ${mydumper_general_options} -- ${myloader_general_options} -d ${myloader_stor_dir} --serialized-table-creation $innodb_optimize_key_mode
 
    # ANSI_QUOTES
#    $test -r 1000 -G ${mydumper_general_options} --defaults-file="test/mydumper.cnf"                                -- ${myloader_general_options} -d ${myloader_stor_dir} --serialized-table-creation --defaults-file="test/mydumper.cnf"
            done
          done
        done
      done
    myloader_stor_dir=$stream_stor_dir
  done
  myloader_stor_dir=$mydumper_stor_dir
}

full_test_per_table(){
  prepare_full_test
  PARTIAL=1
  echo "Starting per table tests"
  for test in test_case_dir test_case_stream
  do
    echo "Executing tests: $test"
    do_case $test -G --lock-all-tables -B empty_db ${mydumper_general_options}                           -- ${myloader_general_options} -d ${myloader_stor_dir} --serialized-table-creation
    # exporting specific database -- overriting database
    do_case $test -B myd_test_no_fk ${mydumper_general_options} -- ${myloader_general_options} -d ${myloader_stor_dir} --serialized-table-creation
    # exporting specific table -- overriting database
    do_case $test -B myd_test -T myd_test.mydumper_aipk_uuid ${mydumper_general_options}	-- ${myloader_general_options} -d ${myloader_stor_dir}
    # exporting specific database -- overriting database
    do_case $test -B myd_test_no_fk ${mydumper_general_options} -- ${myloader_general_options} -B myd_test_2 -d ${myloader_stor_dir} --serialized-table-creation
    do_case $test --no-data -G ${mydumper_general_options} -- ${myloader_general_options} -d ${myloader_stor_dir} --serialized-table-creation
    myloader_stor_dir=$stream_stor_dir
  done
}


full_test(){
  full_test_global
  full_test_per_table
}

full_test &&
  finish

#cat $mydumper_log
#cat $myloader_log
