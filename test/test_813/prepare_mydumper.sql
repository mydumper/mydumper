-- Test case 813: Test --bulk-metadata-prefetch option
-- Creates tables with JSON columns and generated columns to verify prefetch works

DROP DATABASE IF EXISTS test_813;
CREATE DATABASE test_813;
USE test_813;

-- Table with JSON column (should be detected by prefetch)
CREATE TABLE json_data (
    id INT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(100) NOT NULL,
    metadata JSON,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO json_data (name, metadata) VALUES
('item1', '{"type": "A", "value": 100}'),
('item2', '{"type": "B", "value": 200}'),
('item3', '{"type": "A", "value": 150}');

-- Table with generated column (should be detected by prefetch)
CREATE TABLE with_generated (
    id INT PRIMARY KEY AUTO_INCREMENT,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    full_name VARCHAR(101) GENERATED ALWAYS AS (CONCAT(first_name, ' ', last_name)) STORED,
    price DECIMAL(10,2),
    quantity INT,
    total DECIMAL(12,2) GENERATED ALWAYS AS (price * quantity) VIRTUAL
);

INSERT INTO with_generated (first_name, last_name, price, quantity) VALUES
('John', 'Doe', 10.50, 5),
('Jane', 'Smith', 20.00, 3),
('Bob', 'Wilson', 15.75, 10);

-- Regular table (no JSON or generated columns)
CREATE TABLE regular_data (
    id INT PRIMARY KEY AUTO_INCREMENT,
    description TEXT,
    amount DECIMAL(10,2)
);

INSERT INTO regular_data (description, amount) VALUES
('Regular item 1', 100.00),
('Regular item 2', 200.00);

-- Table with different collation (tests collation prefetch)
CREATE TABLE utf8mb4_data (
    id INT PRIMARY KEY AUTO_INCREMENT,
    content VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
);

INSERT INTO utf8mb4_data (content) VALUES
('Hello World'),
('Привет мир'),
('你好世界');
