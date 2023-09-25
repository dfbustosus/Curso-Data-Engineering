-- Momento 1 (creacion de tablas)
CREATE TABLE customers(
    customerid INT primary key,
    name VARCHAR(50),
    occupation VARCHAR(50),
    email VARCHAR(50),
    company VARCHAR(50),
    phonenumber VARCHAR(20),
    age INT
);

CREATE TABLE agents(
    agentid INT primary key,
    name VARCHAR(50)
);

CREATE TABLE calls(
    callid INT primary key,
    agentid INT,
    customerid INT,
    pickedup SMALLINT,
    duration INT,
    productsold SMALLINT
);

-- Momento 2 (Insercion de registros - analogo- OLTP)
SET STATISTICS TIME ON;  
INSERT INTO dbo.calls VALUES (10000, 4,6, 1, 130, 1);
INSERT INTO dbo.calls VALUES (10001, 5,7, 1, 131, 0);
INSERT INTO dbo.calls VALUES (10002, 10,260, 0, 0, 0);
INSERT INTO dbo.calls VALUES (10003, 3,5, 1, 60, 1);
INSERT INTO dbo.calls VALUES (10004, 10,731, 1, 90, 0);
INSERT INTO dbo.calls VALUES (10005, 4,415, 0, 0, 0);
SET STATISTICS TIME OFF;  
GO 

-- PostgreSQL
INSERT INTO calls VALUES (10000, 4,6, 1, 130, 1);
INSERT INTO calls VALUES (10001, 5,7, 1, 131, 0);
INSERT INTO calls VALUES (10002, 10,260, 0, 0, 0);
INSERT INTO calls VALUES (10003, 3,5, 1, 60, 1);
INSERT INTO calls VALUES (10004, 10,731, 1, 90, 0);
INSERT INTO calls VALUES (10005, 4,415, 0, 0, 0);

-- Momento 3 (Generacion de consulta analoga a OLAP)
/*
Esta consulta recupera datos sobre los agentes, los clientes y la duración de sus 
llamadas más rápidas para un producto específico. Aquí hay un desglose de cómo 
funciona la consulta:

1. La instrucción SELECT más externa selecciona el nombre del agente, el nombre del 
cliente y la duración de su llamada más rápida.
2. La cláusula FROM especifica que estamos consultando desde una subconsulta llamada
 "x".
3. La subconsulta "x" usa un JOIN para combinar dos tablas. La primera tabla es una 
subconsulta que selecciona el ID del agente y la duración de su llamada más rápida.
4. La cláusula WHERE filtra las filas solo a aquellas con el producto específico
vendido.
5. La cláusula GROUP BY agrupa los datos por ID de agente y duración, lo que nos
permite encontrar la llamada más rápida para cada agente.
6. Las instrucciones JOIN más externas unen el ID del agente y el ID del cliente
de la subconsulta "x" a las tablas "agentes" y "clientes", respectivamente,
para obtener el nombre del agente y el nombre del cliente.
*/

SET STATISTICS TIME ON;  
SELECT a.name AS AgentName, cu.name AS CustomerName, x.duration
FROM
(
   SELECT ca.agentid, ca.duration, max(customerid) AS cid
   FROM
   (
       SELECT agentid, min(duration) as fastestcall
       FROM calls
       WHERE productsold = 1
       GROUP BY agentid
   ) min
   JOIN calls ca ON ca.agentid = min.agentid AND ca.duration = min.fastestcall
   WHERE productsold = 1
   GROUP BY ca.agentid, ca.duration
) x
JOIN agents a ON x.agentid = a.agentid
JOIN customers cu ON cu.customerid = x.cid
SET STATISTICS TIME OFF;  
GO  

-- PostgreSQL
SELECT a.name AS AgentName, cu.name AS CustomerName, x.duration
FROM
(
   SELECT ca.agentid, ca.duration, max(customerid) AS cid
   FROM
   (
       SELECT agentid, min(duration) as fastestcall
       FROM calls
       WHERE productsold = 1
       GROUP BY agentid
   ) min
   JOIN calls ca ON ca.agentid = min.agentid AND ca.duration = min.fastestcall
   WHERE productsold = 1
   GROUP BY ca.agentid, ca.duration
) x
JOIN agents a ON x.agentid = a.agentid
JOIN customers cu ON cu.customerid = x.cid

-- Explicado paso a paso
-- 1. Selecciona las duraciones minimas tiempo seg de cada vendedor cuando vende
SELECT agentid, min(duration) as fastestcall
       FROM calls
       WHERE productsold = 1
       GROUP BY agentid
-- 2. sacar para cada agentid, duracion minima y id de cliente
(
   SELECT ca.agentid, ca.duration, max(customerid) AS cid
   FROM
   (
       SELECT agentid, min(duration) as fastestcall
       FROM calls
       WHERE productsold = 1
       GROUP BY agentid
   ) min
   JOIN calls ca ON ca.agentid = min.agentid AND ca.duration = min.fastestcall
   WHERE productsold = 1
   GROUP BY ca.agentid, ca.duration
) 
-- 3. Unir con informacion de nombre de agente y cliente
SELECT a.name AS AgentName, cu.name AS CustomerName, x.duration
FROM
(
   SELECT ca.agentid, ca.duration, max(customerid) AS cid
   FROM
   (
       SELECT agentid, min(duration) as fastestcall
       FROM calls
       WHERE productsold = 1
       GROUP BY agentid
   ) min
   JOIN calls ca ON ca.agentid = min.agentid AND ca.duration = min.fastestcall
   WHERE productsold = 1
   GROUP BY ca.agentid, ca.duration
) x
JOIN agents a ON x.agentid = a.agentid
JOIN customers cu ON cu.customerid = x.cid
