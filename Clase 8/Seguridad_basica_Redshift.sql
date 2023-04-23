-- Momento 1
CREATE SCHEMA my_secure_schema;

CREATE TABLE my_secure_schema.my_secure_table (
name VARCHAR(30),
dob TIMESTAMP SORTKEY,
zip INTEGER,
ssn VARCHAR(9)
)
diststyle all;

-- Momento 2
CREATE USER data_scientist PASSWORD 'Test1234';

CREATE GROUP ds_prod WITH USER data_scientist;

-- Momento 3
SELECT * FROM my_secure_schema.my_secure_table;


--- Verificar si tiene acceso al esquema
SELECT schema_name FROM information_schema.schemata 
WHERE schema_owner = 'data_scientist';

-- Verificar todos mis esquemas
SELECT schema_name FROM information_schema.schemata 
WHERE schema_owner = 'dafbustosus_coderhouse';

-- ver todos los usuarios
SELECT usename FROM pg_user

-- Ver permisos del usuario
SELECT grantee, privilege_type 
FROM information_schema.table_privileges 
WHERE grantee = 'data_scientist';
