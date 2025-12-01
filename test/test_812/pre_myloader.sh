#!/bin/bash
# Pre-myloader script: Phase 1 of two-phase loading
# Uses myloader --no-data to create schemas from the dump
# Then the main myloader run tests --no-schema mode (Phase 2)

# Clean up target database completely
mysql --user root -e "DROP DATABASE IF EXISTS test_812"

# Phase 1: Create schemas using myloader --no-data
# This ensures schemas match exactly what was dumped
./myloader --user root --no-data --overwrite-tables --directory=/tmp/data

# IMPORTANT: Ensure tables are completely empty for Phase 2 data load
# --overwrite-tables with --no-schema doesn't TRUNCATE (TRUNCATE happens in schema processing)
# Must explicitly empty tables, with FK checks disabled due to foreign key constraints
mysql --user root -e "SET FOREIGN_KEY_CHECKS=0; TRUNCATE TABLE test_812.items; TRUNCATE TABLE test_812.orders; TRUNCATE TABLE test_812.users; SET FOREIGN_KEY_CHECKS=1;"

echo "Phase 1: Schemas created and tables truncated, ready for --no-schema data load"
