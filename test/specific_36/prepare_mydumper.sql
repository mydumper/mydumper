DROP DATABASE IF EXISTS specific_36;
CREATE DATABASE specific_36;

USE specific_36;

CREATE TABLE only_index_mode (
  id int NOT NULL AUTO_INCREMENT,
  group_id int NOT NULL,
  payload varchar(64) NOT NULL,
  PRIMARY KEY (id),
  KEY idx_group_id (group_id)
);

INSERT INTO only_index_mode (group_id, payload) VALUES
  (10, 'alpha'),
  (10, 'beta'),
  (20, 'gamma'),
  (30, 'delta');
