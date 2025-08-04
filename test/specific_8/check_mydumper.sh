

num_replace=$(grep -i "replace into" /tmp/data/* | wc -l)

if [ $num_replace == 1 ]
then
  exit 0
else
  exit 1
fi

