

num_time_zone=$(grep -i 'TIME_ZONE' /tmp/data/* | wc -l)

if [ $num_time_zone == 0 ]
then
  exit 0
else
  exit 1
fi

