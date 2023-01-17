#!/bin/bash
empty="/tmp/empty"
mydumper_log="/tmp/test_mydumper.log"
tmp_mydumper_log="/tmp/test_mydumper.log.tmp"
myloader_log="/tmp/test_myloader.log"
tmp_myloader_log="/tmp/test_myloader.log.tmp"
mydumper_stor_dir="/tmp/data"
myloader_stor_dir=$mydumper_stor_dir
stream_stor_dir="/tmp/stream_data"
mydumper_base="."
mydumper="${mydumper_base}/mydumper"
myloader="${mydumper_base}/myloader"
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

echo "[mydumper]" > $empty
echo "[myloader]" >> $empty
test_case_dir (){
  # Test case
  # We should consider each test case, with different mydumper/myloader parameters
  s=$*

  mydumper_parameters=${s%%"-- "*}
  myloader_parameters=${s#*"-- "}

  if [ "${mydumper_parameters}" != "" ]
  then
    # Prepare
    rm -rf ${mydumper_stor_dir}
    mkdir -p ${mydumper_stor_dir}
    # Export
    echo "Exporting database: ${mydumper_parameters}"
    eval $mydumper --defaults-file="$empty" -u root -M -v 4 -L $tmp_mydumper_log ${mydumper_parameters}
    error=$?
    cat $tmp_mydumper_log >> $mydumper_log
    if (( $error > 0 ))
    then
      echo "Error running: $mydumper --defaults-file="$empty" -u root -M -v 4 -L $mydumper_log ${mydumper_parameters}"
      cat $tmp_mydumper_log
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

    eval $myloader --defaults-file="$empty" -u root -v 4 -L $tmp_myloader_log ${myloader_parameters}
    error=$?
    cat $tmp_myloader_log >> $myloader_log
    if (( $error > 0 ))
    then
      echo "Error running: $myloader --defaults-file="$empty" -u root -v 4 -L $myloader_log ${myloader_parameters}"
      echo "Error running myloader with mydumper: $mydumper --defaults-file="$empty" -u root -M -v 4 -L $mydumper_log ${mydumper_parameters}"
      cat $tmp_myloader_log
      exit $error
    fi
  fi
}


test_case_stream (){
  # Test case
  # We should consider each test case, with different mydumper/myloader parameters
  s=$*

  mydumper_parameters=${s%%"-- "*}
  myloader_parameters=${s#*"-- "}

  if [ "${mydumper_parameters}" != "" ] && [ "${myloader_parameters}" != "" ]
  then
    # Prepare
    rm -rf ${mydumper_stor_dir} ${myloader_stor_dir}
    mkdir -p ${mydumper_stor_dir} ${myloader_stor_dir}
    # Export
    echo "Exporting database: $mydumper --stream --defaults-file="$empty" -u root -M -v 4 -L $tmp_mydumper_log ${mydumper_parameters} | $myloader --defaults-file="$empty" -u root -v 4 -L $tmp_myloader_log ${myloader_parameters} --stream"
    eval $mydumper --stream --defaults-file="$empty" -u root -M -v 4 -L $tmp_mydumper_log ${mydumper_parameters} > /tmp/stream.sql
    cat /tmp/stream.sql | $myloader --defaults-file="$empty" -u root -v 4 -L $tmp_myloader_log ${myloader_parameters} --stream
    error=$?
    cat $tmp_myloader_log >> $myloader_log
    cat $tmp_mydumper_log >> $mydumper_log
    if (( $error > 0 ))
    then
      echo "Error running: $mydumper --stream --defaults-file="$empty" -u root -M -v 4 -L $mydumper_log ${mydumper_parameters}"
      echo "Error running: $myloader --defaults-file="$empty" -u root -v 4 -L $myloader_log ${myloader_parameters} --stream"
      cat $tmp_mydumper_log
      cat $tmp_myloader_log
      exit $error
    fi
  fi
}


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
  mysql --no-defaults -f -h 127.0.0.1 -u root < mydumper_testing_db.sql


  # export -- import
  # 1000 rows -- database must not exist

  general_options="-h 127.0.0.1 -u root -R -E -o ${mydumper_stor_dir} --regex '^(?!(mysql\.|sys\.))'"



  # single file compressed -- overriting database
#  test_case_dir -c ${general_options}                                 -- -h 127.0.0.1 -o -d ${myloader_stor_dir}
  PARTIAL=0
  for test in test_case_dir test_case_stream
  do 
    echo "Executing test: $test"
    $test -r 1000 -G ${general_options} 				-- -h 127.0.0.1 -o -d ${myloader_stor_dir} --serialized-table-creation
    # 10000 rows -- overriting database
    $test -r 10:100:10000 ${general_options} 				-- -h 127.0.0.1 -o -d ${myloader_stor_dir} --serialized-table-creation --innodb-optimize-keys=AFTER_IMPORT_PER_TABLE
    # chunking the file to 10MB -- overriting database
    $test -F 10 ${general_options} 				-- -h 127.0.0.1 -o -d ${myloader_stor_dir} --serialized-table-creation --innodb-optimize-keys=AFTER_IMPORT_ALL_TABLES
    # chunking the file to 100MB -- overriting database
    $test -F 100 ${general_options} 				-- -h 127.0.0.1 -o -d ${myloader_stor_dir} --serialized-table-creation --innodb-optimize-keys=AFTER_IMPORT_PER_TABLE
    # statement size to 2MB -- overriting database
    $test -s 2000000 ${general_options} 			-- -h 127.0.0.1 -o -d ${myloader_stor_dir} --serialized-table-creation --innodb-optimize-keys=AFTER_IMPORT_ALL_TABLES
    # compress and rows
    $test -r 1000 -c ${general_options}                         -- -h 127.0.0.1 -o -d ${myloader_stor_dir} --serialized-table-creation --innodb-optimize-keys=AFTER_IMPORT_PER_TABLE
    # --load-data
    $test --load-data ${general_options}                        -- -h 127.0.0.1 -o -d ${myloader_stor_dir} --serialized-table-creation --innodb-optimize-keys=AFTER_IMPORT_ALL_TABLES
    # --csv
    $test --csv ${general_options}                              -- -h 127.0.0.1 -o -d ${myloader_stor_dir} --serialized-table-creation --innodb-optimize-keys=AFTER_IMPORT_PER_TABLE
    # --csv
    $test -c --csv ${general_options}                              -- -h 127.0.0.1 -o -d ${myloader_stor_dir} --serialized-table-creation --innodb-optimize-keys=AFTER_IMPORT_ALL_TABLES
    # --csv
    $test -c -r 10000 --csv ${general_options}                              -- -h 127.0.0.1 -o -d ${myloader_stor_dir} --serialized-table-creation  --innodb-optimize-keys=AFTER_IMPORT_PER_TABLE
    # --csv
    $test -c -F 10 --csv ${general_options}                              -- -h 127.0.0.1 -o -d ${myloader_stor_dir} --serialized-table-creation
    myloader_stor_dir=$stream_stor_dir
  done
  myloader_stor_dir=$mydumper_stor_dir
  PARTIAL=1
  echo "Starting per table tests"
  for test in test_case_dir test_case_stream
  do
    echo "Executing tests: $test"
    $test -G --lock-all-tables -B empty_db ${general_options}                           -- -h 127.0.0.1 -o -d ${myloader_stor_dir} --serialized-table-creation
    # exporting specific database -- overriting database
    $test -B myd_test_no_fk ${general_options} -- -h 127.0.0.1 -o -d ${myloader_stor_dir} --serialized-table-creation
    # exporting specific table -- overriting database
    $test -B myd_test -T myd_test.mydumper_aipk_uuid ${general_options}	-- -h 127.0.0.1 -o -d ${myloader_stor_dir}
    # exporting specific database -- overriting database
    $test -B myd_test_no_fk ${general_options} -- -h 127.0.0.1 -o -B myd_test_2 -d ${myloader_stor_dir} --serialized-table-creation
    myloader_stor_dir=$stream_stor_dir
  done


}

full_test

$test --no-data -G ${general_options} 				-- -h 127.0.0.1 -o -d ${myloader_stor_dir} --serialized-table-creation


#cat $mydumper_log
#cat $myloader_log

