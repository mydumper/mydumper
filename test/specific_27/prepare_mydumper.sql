-- Test case 814: Test --skip-metadata-sorting option
-- Creates multiple tables to verify dump works without sorting metadata
-- The metadata file will have unsorted table/database entries

DROP DATABASE IF EXISTS specific_27;
CREATE DATABASE specific_27;
USE specific_27;

-- Create tables with names that would sort differently
-- This tests that unsorted metadata still works correctly
CREATE TABLE zebra (
    id INT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(100)
);

CREATE TABLE alpha (
    id INT PRIMARY KEY AUTO_INCREMENT,
    value INT
);

CREATE TABLE middle (
    id INT PRIMARY KEY AUTO_INCREMENT,
    data TEXT
);

CREATE TABLE beta (
    id INT PRIMARY KEY AUTO_INCREMENT,
    amount DECIMAL(10,2)
);

-- Insert test data
INSERT INTO zebra (name) VALUES ('last'), ('entry');
INSERT INTO alpha (value) VALUES (1), (2), (3);
INSERT INTO middle (data) VALUES ('test data 1'), ('test data 2');
INSERT INTO beta (amount) VALUES (100.00), (200.50);
