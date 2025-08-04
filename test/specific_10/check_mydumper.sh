

num_hex_rows=$(grep -i ",0x" /tmp/data/* | wc -l)

if [ $num_hex_rows == 10 ]
then
  exit 0
else
  exit 1
fi

