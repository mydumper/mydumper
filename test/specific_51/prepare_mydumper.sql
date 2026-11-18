DROP DATABASE IF EXISTS specific_51_source;
DROP DATABASE IF EXISTS specific_51_target;
CREATE DATABASE specific_51_source;
CREATE DATABASE specific_51_target;
CREATE TABLE specific_51_source.t (v INT);
CREATE TABLE specific_51_target.t (v INT);
CREATE VIEW specific_51_source.v AS SELECT 'specific_51_source' AS literal_value, v FROM specific_51_source.t;
CREATE VIEW specific_51_target.v AS SELECT 'specific_51_source' AS literal_value, v FROM specific_51_target.t;
