DROP DATABASE IF EXISTS specific_36;
CREATE DATABASE specific_36;

USE specific_36;

CREATE TABLE only_index_mode (
  id int NOT NULL AUTO_INCREMENT,
  group_id int NOT NULL,
  payload varchar(64) NOT NULL,
  PRIMARY KEY (id)
);

INSERT INTO only_index_mode (id, group_id, payload) VALUES
  (1, 10, 'alpha'),
  (2, 10, 'beta'),
  (3, 20, 'gamma'),
  (4, 30, 'delta');
