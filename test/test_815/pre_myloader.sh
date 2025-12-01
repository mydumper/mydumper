#!/bin/bash
# Pre-myloader script: Create schemas first (Phase 1)
# This simulates the first phase of two-phase loading
# Then the main myloader run will test --no-schema mode (Phase 2)

mysql --user root -e "DROP DATABASE IF EXISTS test_815"
mysql --user root -e "CREATE DATABASE test_815"
mysql --user root -e "
USE test_815;
CREATE TABLE users (
    id INT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(255)
);
CREATE TABLE orders (
    id INT PRIMARY KEY AUTO_INCREMENT,
    user_id INT NOT NULL,
    total DECIMAL(10,2),
    FOREIGN KEY (user_id) REFERENCES users(id)
);
CREATE TABLE items (
    id INT PRIMARY KEY AUTO_INCREMENT,
    order_id INT NOT NULL,
    product VARCHAR(100),
    quantity INT,
    FOREIGN KEY (order_id) REFERENCES orders(id)
);
"
echo "Phase 1: Schemas created, ready for --no-schema data load"
