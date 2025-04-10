

DROP DATABASE IF EXISTS specific_3;
CREATE DATABASE specific_3;

USE specific_3;

CREATE TABLE t_where (id int primary key auto_increment, val int ) engine=InnoDB;

INSERT INTO t_where (id,val) values (1,1),(2,2),(3,3),(4,4),(5,5),(6,6),(7,7),(8,8),(9,9),(10,10);
INSERT INTO t_where (id,val) values (11,1),(12,2),(13,3),(14,4),(15,5),(16,6),(17,7),(18,8),(19,9),(20,10);
