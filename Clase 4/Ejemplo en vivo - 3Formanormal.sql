-- Eliminar la columna ciudad de customers y crear una nueva tabla zips para almacenar esto
ALTER TABLE customers
DROP COLUMN city;


CREATE TABLE zips (
    zip   VARCHAR(5) PRIMARY KEY, 
    city  VARCHAR(255)
);

-- Borrado final
drop table contact_persons;
drop table customers;
drop table project_feedbacks;
drop table zips;