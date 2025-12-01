#!/bin/bash
# Pre-myloader script: Create empty schema for --no-schema mode test
# The main myloader run will use --no-schema to load data only

# Create empty schema for test_812 using -e flag to avoid heredoc issues
mysql --user root -e "DROP DATABASE IF EXISTS test_812"
mysql --user root -e "CREATE DATABASE test_812"
mysql --user root test_812 -e "CREATE TABLE users (id INT PRIMARY KEY AUTO_INCREMENT, name VARCHAR(100) NOT NULL, email VARCHAR(255))"
mysql --user root test_812 -e "CREATE TABLE orders (id INT PRIMARY KEY AUTO_INCREMENT, user_id INT NOT NULL, total DECIMAL(10,2), FOREIGN KEY (user_id) REFERENCES users(id))"
mysql --user root test_812 -e "CREATE TABLE items (id INT PRIMARY KEY AUTO_INCREMENT, order_id INT NOT NULL, product VARCHAR(100), quantity INT, FOREIGN KEY (order_id) REFERENCES orders(id))"
