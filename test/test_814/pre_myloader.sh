#!/bin/bash
# Pre-myloader script: Ensure clean database state before import

mysql --user root -e "DROP DATABASE IF EXISTS test_814"
echo "Database test_814 dropped, myloader will recreate"
