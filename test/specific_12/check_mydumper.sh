

num_definer=$(grep -i "definer" /tmp/data/* | wc -l)

if [ $num_definer == 2 ]
then
  exit 0
else
  exit 1
fi

