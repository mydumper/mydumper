#!/bin/bash
# Pre-myloader script for --no-schema mode testing
set -e

# Drop and recreate database with empty tables
mysql --user root -e "DROP DATABASE IF EXISTS test_815"
mysql --user root -e "CREATE DATABASE test_815"
mysql --user root test_815 -e "CREATE TABLE users (id INT PRIMARY KEY AUTO_INCREMENT, name VARCHAR(100) NOT NULL, email VARCHAR(255))"
mysql --user root test_815 -e "CREATE TABLE orders (id INT PRIMARY KEY AUTO_INCREMENT, user_id INT NOT NULL, total DECIMAL(10,2))"
mysql --user root test_815 -e "CREATE TABLE items (id INT PRIMARY KEY AUTO_INCREMENT, order_id INT NOT NULL, product VARCHAR(100), quantity INT)"

# Verify tables are empty
COUNT=$(mysql --user root -N -e "SELECT COUNT(*) FROM test_815.users")
echo "pre_myloader.sh: tables created, users count=$COUNT"
