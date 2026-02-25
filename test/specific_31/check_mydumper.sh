


LOG=/tmp/test_mydumper.log.tmp

amount_of_threads_1=$(grep '`k1` = 1)' $LOG | cut -d'-' -f4 | cut -d':' -f1 | sort | uniq -c | wc -l)

amount_of_threads_10M=$(grep '`k1` = 10000000' $LOG | cut -d'-' -f4 | cut -d':' -f1 | sort | uniq -c | wc -l)

total_amount_of_threads_for_both=$((grep '`k1` = 1)' $LOG | cut -d'-' -f4 | cut -d':' -f1 ; grep '`k1` = 10000000' $LOG | cut -d'-' -f4 | cut -d':' -f1) | sort | uniq -c | wc -l )

if (( ${amount_of_threads_1} < 2 )) || (( ${amount_of_threads_10M} < 2 ))
then
  if (( ${total_amount_of_threads_for_both} < 3  ))
  then
    exit 1
  else
    exit 0
  fi
else
  exit 0
fi

