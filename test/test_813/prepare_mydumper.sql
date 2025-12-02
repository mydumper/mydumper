-- Test case 813: Test --bulk-metadata-prefetch option
-- Tests collation prefetch which is the core optimization
-- Uses only basic features that work across MySQL/MariaDB/Percona versions

DROP DATABASE IF EXISTS test_813;
CREATE DATABASE test_813;
USE test_813;

-- Table with default collation
CREATE TABLE default_collation (
    id INT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(100) NOT NULL,
    description TEXT
);

INSERT INTO default_collation (name, description) VALUES
('item1', 'First item'),
('item2', 'Second item'),
('item3', 'Third item');

-- Table with explicit utf8mb4 collation (tests collation prefetch)
CREATE TABLE utf8mb4_data (
    id INT PRIMARY KEY AUTO_INCREMENT,
    content VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
);

INSERT INTO utf8mb4_data (content) VALUES
('Hello World'),
('Test data'),
('More content');

-- Table with latin1 collation (another collation to prefetch)
CREATE TABLE latin1_data (
    id INT PRIMARY KEY AUTO_INCREMENT,
    content VARCHAR(255) CHARACTER SET latin1 COLLATE latin1_swedish_ci
);

INSERT INTO latin1_data (content) VALUES
('Latin text 1'),
('Latin text 2');

-- Regular table with multiple columns
CREATE TABLE regular_data (
    id INT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(100),
    amount DECIMAL(10,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO regular_data (name, amount) VALUES
('Regular item 1', 100.00),
('Regular item 2', 200.00),
('Regular item 3', 300.00);
