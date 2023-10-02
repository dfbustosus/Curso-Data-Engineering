-- Amazon Resshift
select * from dafbustosus_coderhouse.customers;

-- Mover esas columnas a una tabla que contenga la información de contacto de las personas
ALTER TABLE customers
DROP COLUMN contact_person;

ALTER TABLE customers
DROP COLUMN contact_person_role;

ALTER TABLE customers
DROP COLUMN phone_number;

select * from dafbustosus_coderhouse.customers c 

-- crear la tabla contact_persons con un id respectivo
CREATE TABLE contact_persons (
    id              INTEGER PRIMARY KEY,
    name            VARCHAR(300),
    role            VARCHAR(300),
    phone_number    VARCHAR(15)
);

select * from dafbustosus_coderhouse.contact_persons;

-- PgAdmin
select * from customers;

-- Mover esas columnas a una tabla que contenga la información de contacto de las personas
ALTER TABLE customers
DROP COLUMN contact_person;

ALTER TABLE customers
DROP COLUMN contact_person_role;

ALTER TABLE customers
DROP COLUMN phone_number;

select * from dafbustosus_coderhouse.customers c 

-- crear la tabla contact_persons con un id respectivo
CREATE TABLE contact_persons (
    id              INTEGER PRIMARY KEY,
    name            VARCHAR(300),
    role            VARCHAR(300),
    phone_number    VARCHAR(15)
);

select * from contact_persons;
