-- CREACION DE TABLAS
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
------------------------


-- Ejercicio 1
/*
De la base de datos dada. Extraer agentes cuyo nombre empieza por M o termina en O.
*/ 
select * from agents
where name like 'M%' or name like '%o'

-- Ejercicio 2
/*De la base de datos dada. Escribir una consulta que produzca una lista, en orden alfabético, 
 de todas las distintas ocupaciones en la tabla Customer que contengan la palabra "Engineer".*/
SELECT DISTINCT Occupation
FROM customers
WHERE Occupation LIKE '%Engineer%'
ORDER BY Occupation

-- Ejercicio 3
/*De la base de datos dada. Escribir una consulta que devuelva el ID del cliente, su nombre y una columna 
nueva llamada “Mayor30” que contenga "Sí" si el cliente tiene más de 30 años y "No" en caso contrario.
 */

SELECT CustomerID, Name,
    CASE
        WHEN Age >= 30 THEN 'Yes'
        WHEN Age <  30 THEN 'No'
        ELSE 'Missing Data'
    END AS Over30
FROM customers
ORDER BY Name desc

-- Ejercicio 4
/* De la base de datos dada. Escribir una consulta que devuelva todas las llamadas realizadas a clientes de la 
profesión de ingeniería y muestre si son mayores o menores de 30, así como si terminaron comprando el producto de esa llamada.
*/
SELECT CallID, Cu.CustomerID, Name, ProductSold,
    CASE
        WHEN Age >= 30 THEN 'Yes'
        WHEN Age <  30 THEN 'No'
        ELSE 'Missing Data'
    END AS Over30
FROM customers Cu
JOIN calls Ca ON Ca.CustomerID = Cu.CustomerID
WHERE Occupation LIKE '%Engineer%'
ORDER BY Name desc

-- Ejercicio 5
/* De la base de datos dada. Escribir dos consultas: 

1- Una que calcule las ventas totales y las llamadas totales realizadas a los clientes de la profesión de ingeniería.
2. Otra que calcule las mismas métricas para toda la base de clientes.
*/

SELECT SUM(ProductSold) AS TotalSales, COUNT(*) AS NCalls 
FROM customers Cu
JOIN calls Ca ON Ca.CustomerID = Cu.CustomerID
WHERE Occupation LIKE '%Engineer%'


SELECT SUM(ProductSold) AS TotalSales, COUNT(*) AS NCalls 
FROM customers Cu
JOIN calls Ca ON Ca.CustomerID = Cu.CustomerID
--WHERE Occupation LIKE '%Engineer%'

-- Ejercicio 6
/* De la base de datos dada. Escribir una consulta que devuelva 
1. Para cada agente: el nombre del agente, la cantidad de llamadas, las llamadas más largas y más cortas, la duración promedio de las llamadas y la cantidad total de productos vendidos. 
2. Nombra las columnas: AgentName, NCalls, Shortest, Longest, AvgDuration y TotalSales
3. Luego ordenar la tabla por: AgentName en orden alfabético.*/

SELECT Name AS AgentName, COUNT(*) AS NCalls, MIN(Duration) AS Shortest, MAX(Duration) AS Longest, 
AVG(Duration) AS AvgDuration, SUM(ProductSold) AS TotalSales
FROM calls  C
    JOIN agents A ON C.AgentID = A.AgentID
WHERE PickeDup = 1
GROUP BY Name
ORDER BY Name ASC


-- Ejercicio 7
/* 
 De la base de datos dada. Escribir una consulta que extraiga dos métricas del desempeño de los agentes de ventas que le interesan a su empresa: 
1- Para cada agente, cuántos segundos en promedio les toma vender un producto cuando tienen éxito.
2. Para cada agente, cuántos segundos en promedio permanecen en el teléfono antes de darse por vencidos cuando no tienen éxito.
*/

SELECT a.name,
SUM(
   CASE
       WHEN productsold = 0 THEN duration  --suma minutos cuando no vende
       ELSE 0
   END)/SUM(
   CASE
       WHEN productsold = 0 THEN 1 -- suma total llamadad cuando no vende
       ELSE 0
   END)
AS avgWhenNotSold , -- nombre priemra columna
SUM(
   CASE
       WHEN productsold = 1 THEN duration --suma minutos cuando vende
       ELSE 0
   END)/SUM(
       CASE WHEN productsold = 1 THEN 1 --suma de total llamadas cuando vende
       ELSE 0
   END)
AS avgWhenSold -- nombre segunda columna
FROM calls c
JOIN agents a ON c.agentid = a.agentid -- join
GROUP BY a.name
ORDER BY a."name" -- order by 1