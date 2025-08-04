

num_insert_ignore=$(grep -i 'INSERT INTO `actor` (`actor_id`,`first_name`,`last_name`,`last_update`)' /tmp/data/* | wc -l)

if [ $num_insert_ignore == 1 ]
then
  exit 0
else
  exit 1
fi

