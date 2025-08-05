

num_schema_files=$(ls /tmp/data/*schema* 2>/dev/null | wc -l)

if [ $num_schema_files == 0 ]
then
  exit 0
else
  exit 1
fi

