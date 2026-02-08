DROP DATABASE IF EXISTS specific_31;
CREATE DATABASE specific_31;

USE specific_31;



CREATE TABLE `multi_column_table` (
  `k1` int NOT NULL,
  `k2` int NOT NULL,
  `val` int DEFAULT NULL,
  PRIMARY KEY (`k1`,`k2`)
);

SET @k1=1;

SET @k2=1;

INSERT INTO multi_column_table (k1,k2,val) VALUES (@k1, @k2, 5);

INSERT IGNORE INTO multi_column_table (k1,k2,val) SELECT k1, pow(2,@k2) + k2, 5 FROM multi_column_table WHERE k1=1; SET @k2=@k2+1; -- k2=1
INSERT IGNORE INTO multi_column_table (k1,k2,val) SELECT k1, pow(2,@k2) + k2, 5 FROM multi_column_table WHERE k1=1; SET @k2=@k2+1; -- k2=2
INSERT IGNORE INTO multi_column_table (k1,k2,val) SELECT k1, pow(2,@k2) + k2, 5 FROM multi_column_table WHERE k1=1; SET @k2=@k2+1; -- k2=3
INSERT IGNORE INTO multi_column_table (k1,k2,val) SELECT k1, pow(2,@k2) + k2, 5 FROM multi_column_table WHERE k1=1; SET @k2=@k2+1; -- k2=4
INSERT IGNORE INTO multi_column_table (k1,k2,val) SELECT k1, pow(2,@k2) + k2, 5 FROM multi_column_table WHERE k1=1; SET @k2=@k2+1; -- k2=5
INSERT IGNORE INTO multi_column_table (k1,k2,val) SELECT k1, pow(2,@k2) + k2, 5 FROM multi_column_table WHERE k1=1; SET @k2=@k2+1; -- k2=6
INSERT IGNORE INTO multi_column_table (k1,k2,val) SELECT k1, pow(2,@k2) + k2, 5 FROM multi_column_table WHERE k1=1; SET @k2=@k2+1; -- k2=7
INSERT IGNORE INTO multi_column_table (k1,k2,val) SELECT k1, pow(2,@k2) + k2, 5 FROM multi_column_table WHERE k1=1; SET @k2=@k2+1; -- k2=8
INSERT IGNORE INTO multi_column_table (k1,k2,val) SELECT k1, pow(2,@k2) + k2, 5 FROM multi_column_table WHERE k1=1; SET @k2=@k2+1; -- k2=9

INSERT IGNORE INTO multi_column_table (k1,k2,val) SELECT @k1, k2, 5 FROM multi_column_table WHERE k1=1; SET @k1=@k1+1;
INSERT IGNORE INTO multi_column_table (k1,k2,val) SELECT @k1, k2, 5 FROM multi_column_table WHERE k1=1; SET @k1=@k1+1;
INSERT IGNORE INTO multi_column_table (k1,k2,val) SELECT @k1, k2, 5 FROM multi_column_table WHERE k1=1; SET @k1=@k1+1;
INSERT IGNORE INTO multi_column_table (k1,k2,val) SELECT @k1, k2, 5 FROM multi_column_table WHERE k1=1; SET @k1=@k1+1;
INSERT IGNORE INTO multi_column_table (k1,k2,val) SELECT @k1, k2, 5 FROM multi_column_table WHERE k1=1; SET @k1=@k1+1;
INSERT IGNORE INTO multi_column_table (k1,k2,val) SELECT @k1, k2, 5 FROM multi_column_table WHERE k1=1; SET @k1=@k1+1;
INSERT IGNORE INTO multi_column_table (k1,k2,val) SELECT @k1, k2, 5 FROM multi_column_table WHERE k1=1; SET @k1=@k1+1;
INSERT IGNORE INTO multi_column_table (k1,k2,val) SELECT @k1, k2, 5 FROM multi_column_table WHERE k1=1; SET @k1=@k1+1;
INSERT IGNORE INTO multi_column_table (k1,k2,val) SELECT @k1, k2, 5 FROM multi_column_table WHERE k1=1; SET @k1=@k1+1;
INSERT IGNORE INTO multi_column_table (k1,k2,val) SELECT @k1, k2, 5 FROM multi_column_table WHERE k1=1; SET @k1=@k1+1;

set @k1=10000;

INSERT IGNORE INTO multi_column_table (k1,k2,val) SELECT @k1, k2, 5 FROM multi_column_table WHERE k1=1; SET @k1=@k1+1;
INSERT IGNORE INTO multi_column_table (k1,k2,val) SELECT @k1, k2, 5 FROM multi_column_table WHERE k1=1; SET @k1=@k1+1;
INSERT IGNORE INTO multi_column_table (k1,k2,val) SELECT @k1, k2, 5 FROM multi_column_table WHERE k1=1; SET @k1=@k1+1;
INSERT IGNORE INTO multi_column_table (k1,k2,val) SELECT @k1, k2, 5 FROM multi_column_table WHERE k1=1; SET @k1=@k1+1;
INSERT IGNORE INTO multi_column_table (k1,k2,val) SELECT @k1, k2, 5 FROM multi_column_table WHERE k1=1; SET @k1=@k1+1;
INSERT IGNORE INTO multi_column_table (k1,k2,val) SELECT @k1, k2, 5 FROM multi_column_table WHERE k1=1; SET @k1=@k1+1;
INSERT IGNORE INTO multi_column_table (k1,k2,val) SELECT @k1, k2, 5 FROM multi_column_table WHERE k1=1; SET @k1=@k1+1;
INSERT IGNORE INTO multi_column_table (k1,k2,val) SELECT @k1, k2, 5 FROM multi_column_table WHERE k1=1; SET @k1=@k1+1;
INSERT IGNORE INTO multi_column_table (k1,k2,val) SELECT @k1, k2, 5 FROM multi_column_table WHERE k1=1; SET @k1=@k1+1;
INSERT IGNORE INTO multi_column_table (k1,k2,val) SELECT @k1, k2, 5 FROM multi_column_table WHERE k1=1; SET @k1=@k1+1;

set @k2=10;
set @k1=1;
INSERT IGNORE INTO multi_column_table (k1,k2,val) SELECT k1, pow(2,@k2) + k2, 5 FROM multi_column_table WHERE k1=1; SET @k2=@k2+1; -- k2=10
INSERT IGNORE INTO multi_column_table (k1,k2,val) SELECT k1, pow(2,@k2) + k2, 5 FROM multi_column_table WHERE k1=1; SET @k2=@k2+1; -- k2=11
INSERT IGNORE INTO multi_column_table (k1,k2,val) SELECT k1, pow(2,@k2) + k2, 5 FROM multi_column_table WHERE k1=1; SET @k2=@k2+1; -- k2=12
INSERT IGNORE INTO multi_column_table (k1,k2,val) SELECT k1, pow(2,@k2) + k2, 5 FROM multi_column_table WHERE k1=1; SET @k2=@k2+1; -- k2=13
INSERT IGNORE INTO multi_column_table (k1,k2,val) SELECT k1, pow(2,@k2) + k2, 5 FROM multi_column_table WHERE k1=1; SET @k2=@k2+1; -- k2=14
INSERT IGNORE INTO multi_column_table (k1,k2,val) SELECT k1, pow(2,@k2) + k2, 5 FROM multi_column_table WHERE k1=1; SET @k2=@k2+1; -- k2=15
INSERT IGNORE INTO multi_column_table (k1,k2,val) SELECT k1, pow(2,@k2) + k2, 5 FROM multi_column_table WHERE k1=1; SET @k2=@k2+1; -- k2=16
INSERT IGNORE INTO multi_column_table (k1,k2,val) SELECT k1, pow(2,@k2) + k2, 5 FROM multi_column_table WHERE k1=1; SET @k2=@k2+1; -- k2=17
INSERT IGNORE INTO multi_column_table (k1,k2,val) SELECT k1, pow(2,@k2) + k2, 5 FROM multi_column_table WHERE k1=1; SET @k2=@k2+1; -- k2=18
INSERT IGNORE INTO multi_column_table (k1,k2,val) SELECT k1, pow(2,@k2) + k2, 5 FROM multi_column_table WHERE k1=1; SET @k2=@k2+1; -- k2=19

set @k2=20;
set @k1=1;
INSERT IGNORE INTO multi_column_table (k1,k2,val) SELECT k1, pow(2,@k2) + k2, 5 FROM multi_column_table WHERE k1=1; SET @k2=@k2+1; -- k2=20
INSERT IGNORE INTO multi_column_table (k1,k2,val) SELECT k1, pow(2,@k2) + k2, 5 FROM multi_column_table WHERE k1=1; SET @k2=@k2+1; -- k2=21
INSERT IGNORE INTO multi_column_table (k1,k2,val) SELECT k1, pow(2,@k2) + k2, 5 FROM multi_column_table WHERE k1=1; SET @k2=@k2+1; -- k2=22
INSERT IGNORE INTO multi_column_table (k1,k2,val) SELECT k1, pow(2,@k2) + k2, 5 FROM multi_column_table WHERE k1=1; SET @k2=@k2+1; -- k2=23
-- INSERT IGNORE INTO multi_column_table (k1,k2,val) SELECT k1, pow(2,@k2) + k2, 5 FROM multi_column_table WHERE k1=1; SET @k2=@k2+1; -- k2=24
-- INSERT IGNORE INTO multi_column_table (k1,k2,val) SELECT k1, pow(2,@k2) + k2, 5 FROM multi_column_table WHERE k1=1; SET @k2=@k2+1; -- k2=25
-- INSERT IGNORE INTO multi_column_table (k1,k2,val) SELECT k1, pow(2,@k2) + k2, 5 FROM multi_column_table WHERE k1=1; SET @k2=@k2+1; -- k2=26
-- INSERT IGNORE INTO multi_column_table (k1,k2,val) SELECT k1, pow(2,@k2) + k2, 5 FROM multi_column_table WHERE k1=1; SET @k2=@k2+1; -- k2=27
-- INSERT IGNORE INTO multi_column_table (k1,k2,val) SELECT k1, pow(2,@k2) + k2, 5 FROM multi_column_table WHERE k1=1; SET @k2=@k2+1; -- k2=28
-- INSERT IGNORE INTO multi_column_table (k1,k2,val) SELECT k1, pow(2,@k2) + k2, 5 FROM multi_column_table WHERE k1=1; SET @k2=@k2+1; -- k2=29



set @k1=100000000;
INSERT IGNORE INTO multi_column_table (k1,k2,val) SELECT @k1, k2, 5 FROM multi_column_table WHERE k1=1; SET @k1=@k1+1;


