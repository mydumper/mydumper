#!/bin/bash
mydumper_log="/tmp/test_mydumper.log"
tmp_mydumper_log="/tmp/test_mydumper.log.tmp"
myloader_log="/tmp/test_myloader.log"
tmp_myloader_log="/tmp/test_myloader.log.tmp"
mydumper_stor_dir="/tmp/data"
mysqldumplog=/tmp/mysqldump.sql
myloader_stor_dir=$mydumper_stor_dir
stream_stor_dir="/tmp/stream_data"
mydumper_base="."
mydumper="${mydumper_base}/mydumper"
myloader="${mydumper_base}/myloader"
export G_DEBUG=fatal-criticals
> $mydumper_log
> $myloader_log
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

test_case_dir (){
  # Test case
  # We should consider each test case, with different mydumper/myloader parameters
  s=$*

  number=$(( $number + 1 ))
  echo "Test #${number}"

  mydumper_parameters=${s%%"-- "*}
  myloader_parameters=${s#*"-- "}

  if [ "${mydumper_parameters}" != "" ]
  then
    # Prepare
    rm -rf ${mydumper_stor_dir}
    mkdir -p ${mydumper_stor_dir}
    # Export
    echo "Exporting database: ${mydumper_parameters}"
    eval $mydumper -u root -M -v 4 -L $tmp_mydumper_log ${mydumper_parameters}
    error=$?
    cat $tmp_mydumper_log >> $mydumper_log
    if (( $error > 0 ))
    then
      mysqldump --no-defaults -f -h 127.0.0.1 -u root --all-databases > $mysqldumplog
      echo "Error running: $mydumper -u root -M -v 4 -L $mydumper_log ${mydumper_parameters}"
      cat $tmp_mydumper_log
      mv $tmp_mydumper_log $mydumper_stor_dir
      exit $error
    fi
  fi
  if [ "$PARTIAL" != "1" ]
  then
  echo "DROP DATABASE IF EXISTS sakila;
DROP DATABASE IF EXISTS myd_test;
DROP DATABASE IF EXISTS myd_test_no_fk;
DROP DATABASE IF EXISTS empty_db;" | mysql --no-defaults -f -h 127.0.0.1 -u root
  fi
  if [ "${myloader_parameters}" != "" ]
  then
    # Import
    echo "Importing database: ${myloader_parameters}"
    mysqldump --no-defaults -f -h 127.0.0.1 -u root --all-databases > $mysqldumplog
    eval $myloader -u root -v 4 -L $tmp_myloader_log ${myloader_parameters}
    error=$?
    cat $tmp_myloader_log >> $myloader_log
    if (( $error > 0 ))
    then
      mv $mysqldumplog $mydumper_stor_dir
      echo "Error running: $myloader -u root -v 4 -L $myloader_log ${myloader_parameters}"
      echo "Error running myloader with mydumper: $mydumper -u root -M -v 4 -L $mydumper_log ${mydumper_parameters}"
      cat $tmp_mydumper_log
      cat $tmp_myloader_log
      mv $tmp_mydumper_log $mydumper_stor_dir
      mv $tmp_myloader_log $mydumper_stor_dir
      exit $error
    fi
  fi
}


test_case_stream (){
  # Test case
  # We should consider each test case, with different mydumper/myloader parameters
  s=$*

  number=$(( $number + 1 ))
  echo "Test #${number}"

  mydumper_parameters=${s%%"-- "*}
  myloader_parameters=${s#*"-- "}

  if [ "${mydumper_parameters}" != "" ] && [ "${myloader_parameters}" != "" ]
  then
    # Prepare
    rm -rf ${mydumper_stor_dir} ${myloader_stor_dir}
    mkdir -p ${mydumper_stor_dir} ${myloader_stor_dir}
    # Export
    echo "Exporting database: $mydumper --stream -u root -M -v 4 -L $tmp_mydumper_log ${mydumper_parameters} | $myloader  ${myloader_general_options} -u root -v 4 -L $tmp_myloader_log ${myloader_parameters} --stream"
    eval $mydumper --stream -u root -M -v 4 -L $tmp_mydumper_log ${mydumper_parameters} > /tmp/stream.sql
    error=$?
    mysqldump --no-defaults -f -h 127.0.0.1 -u root --all-databases > $mysqldumplog
    if (( $error > 0 ))
    then
      echo "Error running: $mydumper --stream -u root -M -v 4 -L $mydumper_log ${mydumper_parameters}"
      cat $tmp_mydumper_log
      mv $tmp_mydumper_log $mydumper_stor_dir
      exit $error
    fi
  if [ "$PARTIAL" != "1" ]
  then
  echo "DROP DATABASE IF EXISTS sakila;
DROP DATABASE IF EXISTS myd_test;
DROP DATABASE IF EXISTS myd_test_no_fk;
DROP DATABASE IF EXISTS empty_db;" | mysql --no-defaults -f -h 127.0.0.1 -u root
  fi
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
      exit $error
    fi
  fi
}

number=0

full_test(){

  if [ ! -f "sakila-db.tar.gz" ]; then
    wget -O sakila-db.tar.gz  https://downloads.mysql.com/docs/sakila-db.tar.gz
  fi
  tar xzf sakila-db.tar.gz
  sed -i 's/last_update TIMESTAMP/last_update TIMESTAMP NOT NULL/g;s/NOT NULL NOT NULL/NOT NULL/g' sakila-db/sakila-schema.sql
  mysql --no-defaults -f -h 127.0.0.1 -u root < sakila-db/sakila-schema.sql
  mysql --no-defaults -f -h 127.0.0.1 -u root < sakila-db/sakila-data.sql

  echo "Import testing database"
  DATABASE=myd_test
  mysql --no-defaults -f -h 127.0.0.1 -u root < test/mydumper_testing_db.sql

  # export -- import
  # 1000 rows -- database must not exist

  mydumper_general_options="-h 127.0.0.1 -u root -R -E -o ${mydumper_stor_dir} --regex '^(?!(mysql\.|sys\.))'"
  myloader_general_options="-h 127.0.0.1 -o --max-threads-for-index-creation=1"


  # single file compressed -- overriting database
#  test_case_dir -c ${mydumper_general_options}                                 -- ${myloader_general_options} -d ${myloader_stor_dir}
  PARTIAL=0
  for test in test_case_dir test_case_stream
  do 
    echo "Executing test: $test"

    $test -r 1000 -G ${mydumper_general_options} 				-- ${myloader_general_options} -d ${myloader_stor_dir} --serialized-table-creation --innodb-optimize-keys=AFTER_IMPORT_ALL_TABLES
    # 10000 rows -- overriting database
    $test -r 1000 --less-locking -G ${mydumper_general_options}                                 -- ${myloader_general_options} -d ${myloader_stor_dir} --serialized-table-creation --innodb-optimize-keys=AFTER_IMPORT_ALL_TABLES
    # 10000 rows -- overriting database
    $test -r 10:100:10000 ${mydumper_general_options} 				-- ${myloader_general_options} -d ${myloader_stor_dir} --serialized-table-creation --innodb-optimize-keys=AFTER_IMPORT_ALL_TABLES
    # chunking the file to 10MB -- overriting database
    $test -F 10 ${mydumper_general_options} 				-- ${myloader_general_options} -d ${myloader_stor_dir} --serialized-table-creation --innodb-optimize-keys=AFTER_IMPORT_ALL_TABLES
    # chunking the file to 100MB -- overriting database
    $test -F 100 ${mydumper_general_options} 				-- ${myloader_general_options} -d ${myloader_stor_dir} --serialized-table-creation --innodb-optimize-keys=AFTER_IMPORT_ALL_TABLES
    # statement size to 2MB -- overriting database
    $test -s 2000000 ${mydumper_general_options} 			-- ${myloader_general_options} -d ${myloader_stor_dir} --serialized-table-creation --innodb-optimize-keys=AFTER_IMPORT_ALL_TABLES
    # compress and rows
    $test -r 1000 -c ${mydumper_general_options}                         -- ${myloader_general_options} -d ${myloader_stor_dir} --serialized-table-creation --innodb-optimize-keys=AFTER_IMPORT_ALL_TABLES 
    # compress and rows
    $test --less-locking -r 1000 -c ${mydumper_general_options}                         -- ${myloader_general_options} -d ${myloader_stor_dir} --serialized-table-creation --innodb-optimize-keys=AFTER_IMPORT_ALL_TABLES
    # compress and rows
    $test --use-savepoints -F 10 --less-locking -r 1000 -c ${mydumper_general_options}                         -- ${myloader_general_options} -d ${myloader_stor_dir} --serialized-table-creation --innodb-optimize-keys=AFTER_IMPORT_ALL_TABLES
    # --load-data
    $test --load-data ${mydumper_general_options}                        -- ${myloader_general_options} -d ${myloader_stor_dir} --serialized-table-creation --innodb-optimize-keys=AFTER_IMPORT_ALL_TABLES
    $test --load-data -c ${mydumper_general_options}                        -- ${myloader_general_options} -d ${myloader_stor_dir} --serialized-table-creation --innodb-optimize-keys=AFTER_IMPORT_ALL_TABLES
    # --csv
    $test --csv ${mydumper_general_options}                              -- ${myloader_general_options} -d ${myloader_stor_dir} --serialized-table-creation --innodb-optimize-keys=AFTER_IMPORT_ALL_TABLES
    # --csv
    $test -c --csv ${mydumper_general_options}                              -- ${myloader_general_options} -d ${myloader_stor_dir} --serialized-table-creation --innodb-optimize-keys=AFTER_IMPORT_ALL_TABLES
    # --csv
    $test -c -r 10000 --csv ${mydumper_general_options}                              -- ${myloader_general_options} -d ${myloader_stor_dir} --serialized-table-creation  --innodb-optimize-keys=AFTER_IMPORT_ALL_TABLES
    # --csv
    $test -c -F 10 --csv ${mydumper_general_options}                              -- ${myloader_general_options} -d ${myloader_stor_dir} --serialized-table-creation --innodb-optimize-keys=AFTER_IMPORT_ALL_TABLES
    # ANSI_QUOTES
#    $test -r 1000 -G ${mydumper_general_options} --defaults-file="test/mydumper.cnf"                                -- ${myloader_general_options} -d ${myloader_stor_dir} --serialized-table-creation --defaults-file="test/mydumper.cnf"

    myloader_stor_dir=$stream_stor_dir
  done
  myloader_stor_dir=$mydumper_stor_dir
  PARTIAL=1
  echo "Starting per table tests"
  for test in test_case_dir test_case_stream
  do
    echo "Executing tests: $test"
    $test -G --lock-all-tables -B empty_db ${mydumper_general_options}                           -- ${myloader_general_options} -d ${myloader_stor_dir} --serialized-table-creation
    # exporting specific database -- overriting database
    $test -B myd_test_no_fk ${mydumper_general_options} -- ${myloader_general_options} -d ${myloader_stor_dir} --serialized-table-creation
    # exporting specific table -- overriting database
    $test -B myd_test -T myd_test.mydumper_aipk_uuid ${mydumper_general_options}	-- ${myloader_general_options} -d ${myloader_stor_dir}
    # exporting specific database -- overriting database
    $test -B myd_test_no_fk ${mydumper_general_options} -- ${myloader_general_options} -B myd_test_2 -d ${myloader_stor_dir} --serialized-table-creation
    myloader_stor_dir=$stream_stor_dir
  done


}

full_test

$test --no-data -G ${mydumper_general_options} 				-- ${myloader_general_options} -d ${myloader_stor_dir} --serialized-table-creation


#cat $mydumper_log
#cat $myloader_log

