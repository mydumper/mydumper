

num_of_inserts=$(grep INSERT /tmp/data/specific_4.t_regex_partition.*.sql | wc -l)

if [ $num_of_inserts == 2  ]
then
  exit 0
else
  exit 1
fi

