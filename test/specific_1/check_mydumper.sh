

num_myisam_tables=$(grep -i myisam /tmp/data/*schema* | wc -l)


exit $num_myisam_tables

exit $1

