SET GLOBAL local_infile=ON;

DROP DATABASE IF EXISTS myd_test;
CREATE DATABASE myd_test;
-- Tables and views
USE myd_test;
source test/mydumper_testing_db_template.sql ;

DROP DATABASE IF EXISTS myd_test_no_fk;
CREATE DATABASE myd_test_no_fk;
USE myd_test_no_fk;
source test/mydumper_testing_db_template.sql ;

ALTER TABLE myd_test_no_fk.pertest_child drop foreign key pertest_child_ibfk_1;

-- Empty database
DROP DATABASE IF EXISTS empty_db;
CREATE DATABASE empty_db;

-- Tablespaces



-- Others
