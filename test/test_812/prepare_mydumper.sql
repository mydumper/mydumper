-- Create test database for LOAD DATA mutex testing
-- This test creates 50 tables and loads them with 16 threads
-- to exercise the mutex synchronization in load_data_mutex_locate()

DROP DATABASE IF EXISTS test_load_data;
CREATE DATABASE test_load_data;
USE test_load_data;

-- Create 50 tables with some data to generate multiple LOAD DATA files
-- Each table has 1000 rows to ensure chunking occurs

DELIMITER //
CREATE PROCEDURE create_test_tables()
BEGIN
    DECLARE i INT DEFAULT 1;
    DECLARE sql_stmt VARCHAR(1000);

    WHILE i <= 50 DO
        SET @sql_stmt = CONCAT('CREATE TABLE t', i, ' (
            id INT PRIMARY KEY AUTO_INCREMENT,
            data VARCHAR(255),
            num INT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )');
        PREPARE stmt FROM @sql_stmt;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;

        -- Insert 1000 rows per table
        SET @sql_stmt = CONCAT('INSERT INTO t', i, ' (data, num)
            SELECT CONCAT(''data_'', seq, ''_table_', i, '''), seq
            FROM (
                SELECT @row := @row + 1 as seq
                FROM (SELECT 0 UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
                      UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) a,
                     (SELECT 0 UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
                      UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) b,
                     (SELECT 0 UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
                      UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) c,
                     (SELECT @row := 0) r
                LIMIT 1000
            ) nums');
        PREPARE stmt FROM @sql_stmt;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;

        SET i = i + 1;
    END WHILE;
END//
DELIMITER ;

CALL create_test_tables();
DROP PROCEDURE create_test_tables;
