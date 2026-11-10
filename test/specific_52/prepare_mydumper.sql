DROP DATABASE IF EXISTS specific_52;
CREATE DATABASE specific_52;
USE specific_52;

CREATE TABLE plain_index (value VARCHAR(100), KEY idx (value));
CREATE TABLE plain_copy LIKE plain_index;
CREATE TABLE unique_index (value VARCHAR(100), UNIQUE KEY idx (value));
CREATE TABLE prefix_10 (value VARCHAR(100), KEY idx (value(10)));
CREATE TABLE prefix_20 (value VARCHAR(100), KEY idx (value(20)));
CREATE TABLE unique_prefix (value VARCHAR(100), UNIQUE KEY idx (value(10)));
CREATE TABLE composite_full (leading_value INT, value VARCHAR(100), KEY idx (leading_value, value));
CREATE TABLE composite_prefix (leading_value INT, value VARCHAR(100), KEY idx (leading_value, value(10)));
