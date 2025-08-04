

num_hex_rows=$(grep -i ",0x" /tmp/data/* | sed 's/,/\n/g' | grep -i "^0x" | wc -l)

if [ $num_hex_rows == 20 ]
then
  exit 0
else
  exit 1
fi

