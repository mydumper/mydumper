source sakila-db/sakila-schema.sql ;
source sakila-db/sakila-data.sql ;

USE sakila;

CREATE EVENT myevent
    ON SCHEDULE AT CURRENT_TIMESTAMP - INTERVAL 1 MINUTE
    DO
      UPDATE myschema.mytable SET mycol = mycol + 1;

