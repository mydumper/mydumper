

num_time_zone=$(grep -i "SET TIME_ZONE='+00:00'" /tmp/data/* | wc -l)

if [ $num_time_zone == 3 ]
then
  exit 0
else
  exit 1
fi

