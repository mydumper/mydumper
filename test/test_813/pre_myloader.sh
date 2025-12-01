#!/bin/bash
# Pre-myloader script: Ensure clean database state before import
# Drop database so myloader's --drop-table option has a clean slate

mysql --user root -e "DROP DATABASE IF EXISTS test_813"
echo "Database test_813 dropped, myloader will recreate"
