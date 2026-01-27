source sakila-db/sakila-schema.sql ;
source sakila-db/sakila-data.sql ;

USE sakila;

CREATE EVENT sakila.myevent
    ON SCHEDULE AT CURRENT_TIMESTAMP + INTERVAL 1 HOUR
    DO
      UPDATE myschema.mytable SET mycol = mycol + 1;

