

num_myisam_tables=$(grep -i myisam /tmp/data/*schema* | wc -l)
num_innodb_tables=$(grep -i innodb /tmp/data/*schema* | wc -l)


num_rows=$(egrep '^INSERT|^,\(' /tmp/data/specific_3.t_where.00000.sql | wc -l)


exit $(( $num_rows != 10 ))

exit $1

