

num_data_files=$(ls /tmp/data/*000*.sql 2>/dev/null | wc -l)

if [ $num_data_files == 0 ]
then
  exit 0
else
  exit 1
fi

