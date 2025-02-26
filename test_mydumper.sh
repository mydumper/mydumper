#!/bin/bash
mydumper_log="/tmp/test_mydumper.log"
tmp_mydumper_log="/tmp/test_mydumper.log.tmp"
myloader_log="/tmp/test_myloader.log"
tmp_myloader_log="/tmp/test_myloader.log.tmp"
mydumper_stor_dir="/tmp/data"
mysqldumplog=/tmp/mysqldump.sql
retries=1

mysql_user=root

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
  if [ -z "${MYSQL_HOST}" ];
  then
    export MYSQL_HOST=127.0.0.1
  fi
  if [ -z "${MYSQL_TCP_PORT}" ];
  then
    export MYSQL_TCP_PORT=3306
  fi
  echo "Using TCP connection to $MYSQL_HOST:$MYSQL_TCP_PORT"
fi

mysqldump()
{
  # mysqldump doesn't respect $MYSQL_HOST
  local host_arg=${MYSQL_HOST:+-h $MYSQL_HOST}
  $mysqldump_exe --no-defaults $host_arg -f --user $mysql_user "$@" || exit
}
export -f mysqldump

mysql()
{
  # mysql seems to respect $MYSQL_HOST
  $mysql_exe --no-defaults -f --user $mysql_user "$@" || exit
}
export -f mysql

# LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libjemalloc.so
# export LD_PRELOAD
export G_DEBUG=fatal-criticals
> $mydumper_log
> $myloader_log

optstring_long="case:,rr-myloader,rr-mydumper,debug,prepare"
optstring_short="c:LDd"

opts=$(getopt -o "${optstring_short}" --long "${optstring_long}" --name "$0" -- "$@") ||
    exit $?
eval set -- "$opts"

unset prepare_only
unset case_num
unset case_repeat
unset rr_myloader
unset rr_mydumper
log_level="--verbose 4"

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
  -L|--rr-myloader)
    myloader="rr record $myloader"
    shift;;
  -D|--rr-mydumper)
    mydumper="rr record $mydumper"
    shift;;
  -d|--debug)
    log_level="--debug"
    shift;;
  -r|--retry)
    retries=$2
    shift 2;;
  --prepare)
    prepare_only=1
    shift;;
  --) shift; break;;
  esac
done

mkdir -p ${mydumper_stor_dir}
rm -rf ${mydumper_stor_dir}


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

  echo "Case #${number}${case_cycle:+:$case_cycle}"
  DIR=$1
  mydumper_default_extra_file="${DIR}/mydumper.cnf"
  myloader_default_extra_file="${DIR}/myloader.cnf"

  myloader_pre_execution="${DIR}/pre_myloader.sh"
  myloader_clean_database="${DIR}/clean_databases.sql"

  mydumper_parameters="$log_level --logfile $tmp_mydumper_log --user $mysql_user --checksum-all --defaults-extra-file=${mydumper_default_extra_file}"
  myloader_parameters="$log_level --logfile $tmp_myloader_log --user $mysql_user                --defaults-extra-file=${myloader_default_extra_file}"

  mydumper_execute=$(grep '[mydumper]' $mydumper_default_extra_file | wc -l )
  myloader_execute=$(grep '[myloader]' $myloader_default_extra_file | wc -l )

  mydumper_stream=$(grep 'stream=' $mydumper_default_extra_file | wc -l )
  myloader_stream=$(grep 'stream=' $myloader_default_extra_file | wc -l )

  iter=1
  error=0
  if (( ${mydumper_execute} > 0 ))
  then
    while (( $iter <= $retries ))
    do
      
      # Prepare
      rm -rf ${mydumper_stor_dir}
      # Export
      echo "Exporting database: ${mydumper_parameters}"
      if (( $mydumper_stream >= 1 ))
      then
        "${time2[@]}" $mydumper ${mydumper_parameters} > /tmp/stream.sql
      else
        "${time2[@]}" $mydumper ${mydumper_parameters}
      fi
      error=$?
      cat $tmp_mydumper_log >> $mydumper_log
      if (( $error > 0 ))
      then
        print_core
      fi
      iter=$(( $iter + 1 ))
    done
    if (( $error > 0 )) && (( $iter > $retries ))
    then
      mysqldump --all-databases > $mysqldumplog
      echo "Error running: $mydumper ${mydumper_parameters}"
      #cat $tmp_mydumper_log
      mv $tmp_mydumper_log $mydumper_stor_dir
      backtrace
      exit $error
    fi
  fi
  if [ -f $myloader_clean_database ]
  then
    mysql < $myloader_clean_database
  else
    mysql < test/clean_databases.sql
  fi
  if [ -f $myloader_pre_execution ]
  then
    "$myloader_pre_execution"
  fi

  if (( ${myloader_execute} > 0 ))
  then
    iter=1
    while (( $iter <= $retries ))
    do
      # Import
      echo "Importing database: ${myloader_parameters}"
      mysqldump --all-databases > $mysqldumplog
      if (( $myloader_stream >= 1 ))
      then
        "${time2[@]}" $myloader ${myloader_parameters} < /tmp/stream.sql
      else
        "${time2[@]}" $myloader ${myloader_parameters} 
      fi
      error=$?
      cat $tmp_myloader_log >> $myloader_log
      if (( $error > 0 ))
      then
        print_core
      fi
      iter=$(( $iter + 1 ))
    done
    if (( $error > 0 )) && (( $iter > $retries ))
    then
      mv $mysqldumplog $mydumper_stor_dir
      echo "Error running: $myloader ${myloader_parameters}"
      echo "Error running myloader with mydumper: $mydumper ${mydumper_parameters}"
      cat $tmp_mydumper_log
      cat $tmp_myloader_log
      mv $tmp_mydumper_log $mydumper_stor_dir
      mv $tmp_myloader_log $mydumper_stor_dir
      backtrace
      exit $error
    fi
  fi
}

do_case()
{
  number=$( echo "$2" | cut -d'_' -f2 )
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
  if [[ -n "$prepare_only"  ]]; then
    exit
  fi
}

full_test_global(){
  prepare_full_test
  for dir in $(find test -name "test_*" -maxdepth 1 -mindepth 1 -type d | sort -t '_' -k 2 -n )  
  do 
    echo "Executing test: $dir"
    do_case test_case_dir ${dir}
  done
}

full_test(){
  full_test_global
}

full_test &&
  finish

#cat $mydumper_log
#cat $myloader_log
