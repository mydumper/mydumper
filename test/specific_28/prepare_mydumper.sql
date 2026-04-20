-- Test case for --no-schema mode fix
-- Tests two-phase loading: Phase 1 creates schemas, Phase 2 loads data only
-- Without the fix, Phase 2 would fail because database->schema_state is never CREATED

source test/specific_28/prepare_myloader.sql

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
