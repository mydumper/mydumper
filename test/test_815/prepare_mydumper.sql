-- Test case for --no-schema mode fix
-- Tests two-phase loading: Phase 1 creates schemas, Phase 2 loads data only
-- Without the fix, Phase 2 would fail because database->schema_state is never CREATED

DROP DATABASE IF EXISTS test_815;
CREATE DATABASE test_815;
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

-- Insert test data
INSERT INTO users (name, email) VALUES
    ('Alice', 'alice@example.com'),
    ('Bob', 'bob@example.com'),
    ('Charlie', 'charlie@example.com');

INSERT INTO orders (user_id, total) VALUES
    (1, 100.00),
    (1, 250.50),
    (2, 75.25);

INSERT INTO items (order_id, product, quantity) VALUES
    (1, 'Widget A', 2),
    (1, 'Widget B', 1),
    (2, 'Gadget X', 5),
    (3, 'Widget A', 3);
