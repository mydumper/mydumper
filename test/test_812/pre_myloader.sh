#!/bin/bash
# Pre-myloader script: Phase 1 of two-phase loading
# Uses myloader --no-data to create schemas from the dump
# Then the main myloader run tests --no-schema mode (Phase 2)

# Clean up target database completely
mysql --user root -e "DROP DATABASE IF EXISTS test_812"

# Phase 1: Create schemas using myloader --no-data
# This ensures schemas match exactly what was dumped (empty tables)
# Using --drop-table to ensure clean slate
./myloader --user root --no-data --drop-table --directory=/tmp/data

echo "Phase 1: Empty schemas created, ready for --no-schema data load"
