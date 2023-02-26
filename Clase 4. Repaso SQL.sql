/*Ejercicio 1
Escriba una consulta que seleccione el ID y el nombre del cliente de la tabla Customer, 
mostrando solo los resultados de los clientes que no están desempleados. Recuerde escribir 
su consulta como una cadena de varias líneas (encerrada entre un par de comillas triples """)
 y pasarla a la función runQuery()definida en el marco anterior para verificar su trabajo.*/ 

SELECT CustomerID, Name ,Occupation
FROM customers c 
WHERE Occupation != 'Unemployed'

/* ----------------------- */
-- Aliasing

SELECT CustomerID, Name AS CustomerName
FROM customers c 
WHERE Occupation != 'Unemployed'
ORDER BY CustomerName ASC

/* ----------------------- */
/*Ejercicio 2
Escriba una consulta que produzca una lista, en orden alfabético, de todas las distintas
 ocupaciones en la tabla Customer que contengan la palabra" Ingeniero "*/

SELECT DISTINCT Occupation
FROM customers c 
WHERE Occupation LIKE '%Engineer%'
ORDER BY Occupation DESC

/* ----------------------- */
/*Ejercicio 3
Escriba una consulta que devuelva el ID del cliente, su nombre y una columna Over30 que 
contenga" Sí "si el cliente tiene más de 30 años y" No "si no.
*/

SELECT CustomerID, Name,
    CASE
        WHEN Age >= 30 THEN 'Yes'
        WHEN Age <  30 THEN 'No'
        ELSE 'Missing Data'
    END AS Over30
FROM customers c 
ORDER BY Name DESC


-- Modifiquemos ahora el ejercicio 3 para que la consulta solo devuelva clientes que trabajan en 
-- una profesión de ingeniería:

/* ----------------------- */
SELECT CustomerID, Name,
    CASE
        WHEN Age >= 30 THEN 'Yes'
        WHEN Age <  30 THEN 'No'
        ELSE 'Missing Data'
    END AS Over30
FROM Customer
WHERE Occupation LIKE '%Engineer%'
ORDER BY Name DESC

-- JOINS
/* ----------------------- */
SELECT CallID, a.AgentID, name
FROM calls c 
JOIN agents a ON c.AgentID = a.AgentID
ORDER BY Name DESC


/* ----------------------- */
/* Ejercicio 4
Escriba una consulta que devuelva todas las llamadas realizadas a clientes de la profesión de
ingeniería y muestre si son mayores o menores de 30, así como si terminaron comprando el 
producto de esa llamada
*/
SELECT CallID, c.CustomerID, Name, ProductSold,
    CASE
        WHEN Age >= 30 THEN 'Yes'
        WHEN Age <  30 THEN 'No'
        ELSE 'Missing Data'
    END AS Over30
FROM customers c 
JOIN calls ca ON ca.CustomerID = c.CustomerID
WHERE Occupation LIKE '%Engineer%'
ORDER BY Name DESC


/* ----------------------- */
/*Ejercicio 5
Escriba dos consultas: una que calcule las ventas totales y las llamadas totales realizadas a los 
clientes de la profesión de ingeniería y otra que calcule las mismas métricas para toda la base de 
clientes. ¿Qué puede concluir con respecto a la tasa de conversión entre los clientes de ingeniería 
frente a la base de clientes en general?
*/

SELECT SUM(ProductSold) Ventas, COUNT(*) Conteo
FROM Customer Cu
JOIN Call Ca ON Ca.CustomerID = Cu.CustomerID
WHERE Occupation  LIKE '%Engineer%'

-- Otras condiciones
/* ----------------------- */
SELECT SUM(ProductSold) Ventas, COUNT(*) Conteo
FROM customers c 
JOIN calls ca  ON ca.CustomerID = c.CustomerID
WHERE Occupation NOT LIKE '%Engineer%'

-- Que pasa con esto ahora?
/* ----------------------- */
SELECT SUM(ProductSold) Ventas, COUNT(*) Conteo
FROM customers c 
JOIN calls ca ON ca.CustomerID = c.CustomerID
-- WHERE Occupation  LIKE '%Engineer%'



/* ----------------------- */
/*Ejercicio 6
Escriba una consulta que calcule las ventas totales y las llamadas totales realizadas a 
clientes mayores de 30 años. ¿Existe una diferencia notable entre la tasa de conversión 
aquí y la de la base de clientes en general?
*/

SELECT SUM(ProductSold) AS TotalSales, COUNT(*) AS NCalls
FROM customers c 
JOIN calls c2  ON c2.CustomerID = c.CustomerID
WHERE Age >= 30

/* ----------------------- */
/*Ejercicio 7
¿Qué tal si observa la tasa de conversión de ventas para ingenieros mayores de 30 años?
*/

SELECT SUM(ProductSold) AS TotalSales, COUNT(*) AS NCalls
FROM customers c2 
JOIN calls c  ON c.CustomerID = c2.CustomerID
WHERE (Occupation LIKE '%Engineer%') AND (Age >= 30)


/* ----------------------- */
SELECT SUM(ProductSold) AS TotalSales, COUNT(*) AS NCalls
FROM Customer Cu
JOIN Call Ca ON Ca.CustomerID = Cu.CustomerID
WHERE (Occupation LIKE '%Engineer%') AND (Age < 30)


/* ----------------------- */
/*Ejercicio 8
Escriba una consulta que devuelva, para cada agente, el nombre del agente, la cantidad de llamadas, 
las llamadas más largas y más cortas, la duración promedio de las llamadas y la cantidad total de 
productos vendidos. Nombra las columnas AgentName, NCalls, Shortest, Longest, AvgDuration y TotalSales 
y ordena la tabla por AgentName en orden alfabético. (Asegúrese de incluir la cláusula WHERE PickedUp = 1 
para calcular solo el promedio de todas las llamadas que fueron atendidas (de lo contrario, ¡todas las 
duraciones mínimas serán 0)!)
*/

SELECT Name AS AgentName, COUNT(*) AS NCalls, MIN(Duration) AS Shortest, MAX(Duration) AS Longest, AVG(Duration) AS AvgDuration, SUM(ProductSold) AS TotalSales
FROM Call C
    JOIN Agent A ON C.AgentID = A.AgentID
WHERE PickeDup = 1
GROUP BY Name
ORDER BY Name ASC