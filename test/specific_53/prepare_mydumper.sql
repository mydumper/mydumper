DROP DATABASE IF EXISTS specific_53;
CREATE DATABASE specific_53;
USE specific_53;

CREATE TABLE indexed (
  id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  a INT,
  b VARCHAR(50),
  KEY idx_a (a),
  UNIQUE KEY idx_b (b)
) ENGINE=InnoDB;
INSERT INTO indexed (a, b) VALUES (1, 'x'), (2, 'y');
