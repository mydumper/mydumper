

num_insert_ignore=$(grep -i "replace into" /tmp/data/* | wc -l)

if [ $num_insert_ignore == 1 ]
then
  exit 0
else
  exit 1
fi

