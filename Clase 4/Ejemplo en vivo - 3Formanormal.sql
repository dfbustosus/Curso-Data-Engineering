-- Eliminar la columna ciudad de customers y crear una nueva tabla zips para almacenar esto
ALTER TABLE customers
DROP COLUMN city;


CREATE TABLE zips (
    zip   VARCHAR(5) PRIMARY KEY, 
    city  VARCHAR(255)
);