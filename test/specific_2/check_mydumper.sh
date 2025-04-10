

num_myisam_tables=$(grep -i myisam /tmp/data/*schema* | wc -l)
num_innodb_tables=$(grep -i innodb /tmp/data/*schema* | wc -l)

exit $(( $num_myisam_tables + $num_innodb_tables ))

exit $1

