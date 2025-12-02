-- Test case for schema-data race condition
-- This creates many small tables to maximize the chance of
-- data loading starting before schema creation completes

DROP DATABASE IF EXISTS race_test;
CREATE DATABASE race_test;
USE race_test;

-- Create 100 tables with simple structure
-- The goal is to have many tables created quickly so
-- the race condition between schema and data workers manifests

DELIMITER //

CREATE PROCEDURE create_test_tables()
BEGIN
    DECLARE i INT DEFAULT 1;
    DECLARE sql_stmt VARCHAR(1000);

    WHILE i <= 100 DO
        SET sql_stmt = CONCAT(
            'CREATE TABLE table_', LPAD(i, 3, '0'), ' (',
            'id INT PRIMARY KEY AUTO_INCREMENT, ',
            'name VARCHAR(100), ',
            'value INT, ',
            'created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP',
            ')'
        );
        SET @sql = sql_stmt;
        PREPARE stmt FROM @sql;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;

        -- Insert some rows to ensure data files are created
        SET sql_stmt = CONCAT(
            'INSERT INTO table_', LPAD(i, 3, '0'), ' (name, value) VALUES ',
            '("row1", 100), ("row2", 200), ("row3", 300), ("row4", 400), ("row5", 500)'
        );
        SET @sql = sql_stmt;
        PREPARE stmt FROM @sql;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;

        SET i = i + 1;
    END WHILE;
END //

DELIMITER ;

CALL create_test_tables();
DROP PROCEDURE create_test_tables;
