
drop database if exists specific_21;
create database specific_21;
use specific_21;
CREATE TABLE test (
	test BINARY(16) NOT NULL
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_general_ci;

create or replace
algorithm = UNDEFINED view `test_view` as
select test.*, concat('CONTACT', bin_to_uuid(`test`.`test`)) collate utf8mb4_general_ci from specific_21.test ;
-- select test.*, concat('CONTACT', bin_to_uuid(`test`.`test`)) from specific_21.test ;
