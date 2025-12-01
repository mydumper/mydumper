#!/bin/bash
# Pre-myloader script: Ensure clean database state before import
# Drop and recreate database to avoid "table already exists" or duplicate key errors

mysql --user root -e "DROP DATABASE IF EXISTS test_813"
mysql --user root -e "CREATE DATABASE test_813"
echo "Database test_813 recreated, ready for myloader import"
