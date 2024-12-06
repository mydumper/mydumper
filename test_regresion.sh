function exec_test_without_L() {
  s=$*
  mydumper_parameters=${s%%"-- "*}
  myloader_parameters=${s#*"-- "}
  log=mydumper.${version}.log
  ./mydumper -L ${log} --kill-long-queries ${mydumper_parameters}
  echo "Backup Timing for ${version} with ${mydumper_parameters}: $(( $(date --date="$(tail $log | grep 2024 | tail -1 | cut -d' ' -f 1,2)" +%s) - $(date --date="$(head -1 $log | cut -d' ' -f 1,2)" +%s)   ))"

  sleep 20

  log=myloader.${version}.log
#  ./myloader ${myloader_parameters}  > ${log} 2>&1
  sed -i 's/** Message:/2024-01-01/g' ${log}
  echo "Restor Timing for ${version} with ${myloader_parameters}: $(( $(date --date="$(tail -20 $log | grep 2024 | tail -1 | cut -d' ' -f 1,2 | cut -d'.' -f 1)" +%s) - $(date --date="$(head -1 $log | cut -d' ' -f 1,2 | cut -d'.' -f 1)" +%s)   ))"

  sleep 20
}

function exec_test_with_L() {
  s=$*
  mydumper_parameters=${s%%"-- "*}
  myloader_parameters=${s#*"-- "}
  log=mydumper.${version}.log
  ./mydumper -L ${log} --kill-long-queries ${mydumper_parameters} ;
  echo "Backup Timing for ${version} with ${mydumper_parameters}: $(( $(date --date="$(tail $log | grep 2024 | tail -1 | cut -d' ' -f 1,2)" +%s) - $(date --date="$(head -1 $log | cut -d' ' -f 1,2)" +%s)   ))"

  sleep 20

  log=myloader.${version}.log
#  ./myloader -L ${log} ${myloader_parameters}
  echo "Restore Timing for ${version} with ${myloader_parameters}: $(( $(date --date="$(tail -20 $log | grep 2024 | tail -1 | cut -d' ' -f 1,2)" +%s) - $(date --date="$(head -1 $log | cut -d' ' -f 1,2)" +%s)   ))"

  sleep 20
}

database=sbtest
new_database=sbtest_2


declare -A configuration
basic=" -h 127.0.0.1 -u root -B ${database} -o data -v 3 -r 10000  -- -B ${new_database} -d data -v 3 -o "
configuration["tags/v0.9.3"]=$basic
configuration["tags/v0.9.5"]=$basic
configuration["tags/v0.10.1"]=$basic
configuration["tags/v0.10.7-2"]=$basic
configuration["tags/v0.11.1-6"]=$basic
configuration["tags/v0.11.3-5"]=$basic
configuration["tags/v0.11.5-2"]=$basic
configuration["tags/v0.12.1"]=$basic
configuration["tags/v0.12.3-3"]=$basic
configuration["tags/v0.12.5-3"]=$basic
configuration["tags/v0.12.7-3"]=$basic
configuration["tags/v0.13.1-2"]=$basic
configuration["tags/v0.14.1-1"]=$basic
configuration["tags/v0.14.3-1"]=$basic
configuration["tags/v0.14.5-3"]=$basic
basic=" -h 127.0.0.1 -u root -B ${database} -o data -v 3 -r 10000 -F 5 -- -B ${new_database} -d data -v 3 -o "
configuration["tags/v0.15.1-3"]=$basic
configuration["tags/v0.16.1-3"]=$basic
configuration["tags/v0.16.3-6"]=$basic
configuration["tags/v0.16.5-1"]=$basic
basic=" -h 127.0.0.1 -u root -B ${database} -o data -v 4 -F 50 -- -B ${new_database} -d data -v 3 -o"
configuration["tags/v0.16.7-5"]=$basic
configuration["origin/master"]=$basic
configuration["origin/write_buffer_and_performance_regression"]=$basic

declare -A execute_test
execute_test["tags/v0.9.3"]=exec_test_without_L
execute_test["tags/v0.9.5"]=exec_test_without_L
execute_test["tags/v0.10.1"]=exec_test_without_L
execute_test["tags/v0.10.7-2"]=exec_test_with_L
execute_test["tags/v0.11.1-6"]=exec_test_with_L
execute_test["tags/v0.11.3-5"]=exec_test_with_L
execute_test["tags/v0.11.5-2"]=exec_test_with_L
execute_test["tags/v0.12.1"]=exec_test_with_L
execute_test["tags/v0.12.3-3"]=exec_test_with_L
execute_test["tags/v0.12.5-3"]=exec_test_with_L
execute_test["tags/v0.12.7-3"]=exec_test_with_L
execute_test["tags/v0.13.1-2"]=exec_test_with_L
execute_test["tags/v0.14.1-1"]=exec_test_with_L
execute_test["tags/v0.14.3-1"]=exec_test_with_L
execute_test["tags/v0.14.5-3"]=exec_test_with_L
execute_test["tags/v0.15.1-3"]=exec_test_with_L
execute_test["tags/v0.16.1-3"]=exec_test_with_L
execute_test["tags/v0.16.3-6"]=exec_test_with_L
execute_test["tags/v0.16.5-1"]=exec_test_with_L
execute_test["tags/v0.16.7-5"]=exec_test_with_L
execute_test["origin/master"]=exec_test_with_L
execute_test["origin/write_buffer_and_performance_regression"]=exec_test_with_L
# for git_version in tags/v0.9.3 tags/v0.9.5 tags/v0.10.1 tags/v0.10.7-2 tags/v0.11.1-6 tags/v0.11.3-5 tags/v0.11.5-2
for git_version in tags/v0.9.3 tags/v0.9.5 tags/v0.10.1 tags/v0.10.7-2 tags/v0.11.1-6 tags/v0.11.3-5 tags/v0.11.5-2 tags/v0.12.1 tags/v0.12.3-3 tags/v0.12.5-3 tags/v0.12.7-3  tags/v0.13.1-2 tags/v0.14.1-1 tags/v0.14.3-1 tags/v0.14.5-3 tags/v0.15.1-3 tags/v0.16.1-3 tags/v0.16.3-6 tags/v0.16.5-1 tags/v0.16.7-5 origin/master
# for git_version in tags/v0.16.3-6 tags/v0.16.5-1 tags/v0.16.7-5 origin/master
#for git_version in tags/v0.15.1-3 tags/v0.16.1-3 tags/v0.16.3-6 tags/v0.16.5-1 tags/v0.16.7-5 origin/master
#for git_version in tags/v0.9.5 origin/master
#for git_version in origin/write_buffer_and_performance_regression  tags/v0.9.5 origin/master
do
  rm -rf */data
  version=$( echo ${git_version} | cut -f2 -d'/' )
  if [ ! -d ${version} ]; then
    git clone https://github.com/mydumper/mydumper.git ${version} > /dev/null 2>&1
    cd $version
    git checkout ${git_version} > /dev/null 2>&1
    error=$?
    if (( $error > 0 ))
    then
      echo "Error during checkout $error"
      exit
    fi
    cmake . > /dev/null 2>&1
    make > /dev/null 2>&1
    error=$?
    if (( $error > 0 ))
    then
      echo "Error compiling $error"
      exit
    fi
  else
    cd $version
  fi
  rm -rf data;
  "${execute_test[${git_version}]}" ${configuration[${git_version}]} || exit
  cd ..
done
