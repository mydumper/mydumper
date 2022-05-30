#!/bin/bash
empty="/tmp/empty"
mydumper_log="/tmp/test_mydumper.log"
tmp_mydumper_log="/tmp/test_mydumper.log.tmp"
myloader_log="/tmp/test_myloader.log"
tmp_myloader_log="/tmp/test_myloader.log.tmp"
mydumper_stor_dir="/tmp/data"
myloader_stor_dir=$mydumper_stor_dir
stream_stor_dir="/tmp/stream_data"
mydumper="./mydumper"
myloader="./myloader"
> $mydumper_log
> $myloader_log
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
      cat $mydumper_log
      exit $error
    fi
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
      cat $myloader_log
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
    echo "Exporting database: ${mydumper_parameters} | ${myloader_parameters}"
    $mydumper --stream --defaults-file="$empty" -u root -M -v 4 -L $tmp_mydumper_log ${mydumper_parameters} | $myloader --defaults-file="$empty" -u root -v 4 -L $tmp_myloader_log ${myloader_parameters} --stream
    error=$?
    cat $tmp_myloader_log >> $myloader_log
    cat $tmp_mydumper_log >> $mydumper_log
    if (( $error > 0 ))
    then
      echo "Error running: $mydumper --stream --defaults-file="$empty" -u root -M -v 4 -L $mydumper_log ${mydumper_parameters}"
      echo "Error running: $myloader --defaults-file="$empty" -u root -v 4 -L $myloader_log ${myloader_parameters} --stream"
      cat $mydumper_log
      cat $myloader_log
      exit $error
    fi
  fi
}


full_test(){

  echo "Import testing database"
  DATABASE=myd_test
  mysql --no-defaults -f -h 127.0.0.1 -u root < mydumper_testing_db.sql


  # export -- import
  # 1000 rows -- database must not exist

  general_options="-h 127.0.0.1 -u root -R -E -o ${mydumper_stor_dir} --regex '^(?!(mysql\.|sys\.))'"



  # single file compressed -- overriting database
  test_case_dir -c ${general_options}                                 -- -o -d ${myloader_stor_dir}


  for test in test_case_dir test_case_stream
  do
    $test -r 1000 -G ${general_options} 				-- -h 127.0.0.1 -o -d ${myloader_stor_dir} --serialized-table-creation
    # 10000 rows -- overriting database
    $test -r 10000 ${general_options} 				-- -h 127.0.0.1 -o -d ${myloader_stor_dir} --serialized-table-creation
    # chunking the file to 10MB -- overriting database
    $test -F 10 ${general_options} 				-- -h 127.0.0.1 -o -d ${myloader_stor_dir} --serialized-table-creation
    # chunking the file to 100MB -- overriting database
    $test -F 100 ${general_options} 				-- -h 127.0.0.1 -o -d ${myloader_stor_dir} --serialized-table-creation
    # statement size to 2MB -- overriting database
    $test -s 2000000 ${general_options} 			-- -h 127.0.0.1 -o -d ${myloader_stor_dir} --serialized-table-creation
    # exporting specific database -- overriting database
    $test -B myd_test_no_fk ${general_options} -- -h 127.0.0.1 -o -d ${myloader_stor_dir}
    # exporting specific table -- overriting database
    $test -B myd_test -T myd_test.mydumper_aipk_uuid ${general_options}	-- -h 127.0.0.1 -o -d ${myloader_stor_dir}
    # compress and rows
    $test -r 1000 -c ${general_options}                         -- -h 127.0.0.1 -o -d ${myloader_stor_dir} --serialized-table-creation
    myloader_stor_dir=$stream_stor_dir
  done
}

full_test
