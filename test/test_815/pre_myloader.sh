#!/bin/bash
# Pre-myloader script for --no-schema mode testing
#
# Test flow:
# 1. prepare_mydumper.sql creates test_815 with data
# 2. mydumper dumps to /tmp/test_815_data
# 3. This script (pre_myloader.sh) drops and recreates empty tables
# 4. myloader --no-schema loads data-only from /tmp/test_815_data
#
# The test validates that --no-schema mode correctly sets database->schema_state
# to CREATED even when skipping schema processing.

# Drop and recreate database with empty tables
mysql --user root -e "DROP DATABASE IF EXISTS test_815"
mysql --user root -e "CREATE DATABASE test_815"
mysql --user root test_815 -e "CREATE TABLE users (id INT PRIMARY KEY AUTO_INCREMENT, name VARCHAR(100) NOT NULL, email VARCHAR(255))"
mysql --user root test_815 -e "CREATE TABLE orders (id INT PRIMARY KEY AUTO_INCREMENT, user_id INT NOT NULL, total DECIMAL(10,2))"
mysql --user root test_815 -e "CREATE TABLE items (id INT PRIMARY KEY AUTO_INCREMENT, order_id INT NOT NULL, product VARCHAR(100), quantity INT)"
echo "Phase 1: Empty schemas created, ready for --no-schema data load"
