#!/bin/bash
# Pre-myloader script: Ensure clean database state before import

mysql --user root -e "DROP DATABASE IF EXISTS specific_27"
echo "Database specific_27 dropped, myloader will recreate"
