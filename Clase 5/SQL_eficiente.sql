/*
La consulta primero crea una tabla temporal denominada #Product, que incluye los 25 productos principales de la tabla
 Production.Product, junto con sus nombres, números de producto, categoría y subcategoría de producto e ID de modelo
de producto. Los productos están ordenados por su ModifiedDate en orden descendente.

La segunda parte de la consulta une la tabla #Producto con otras tablas de la base de datos para recuperar información
adicional sobre cada producto. Los datos incluyen el ID del producto, el nombre del producto, el número del producto,
el código de la unidad de medida para el costo, el nombre de la unidad de medida, el tiempo de entrega promedio, 
el precio estándar, el nombre del revisor, la calificación, la categoría del producto y la subcategoría del producto.

La consulta utiliza inner joins, left joins y alias para combinar datos de varias tablas. 
Los resultados se devuelven en formato de tabla. Finalmente, la tabla temporal se elimina al final de la consulta.
*/

SET STATISTICS TIME ON
GO
SELECT TOP 25
	Product.ProductID,
	Product.Name AS ProductName,
	Product.ProductNumber,
	ProductCategory.Name AS ProductCategory,
	ProductSubCategory.Name AS ProductSubCategory,
	Product.ProductModelID
INTO #Product
FROM Production.Product
INNER JOIN Production.ProductSubCategory
ON ProductSubCategory.ProductSubcategoryID = Product.ProductSubcategoryID
INNER JOIN Production.ProductCategory
ON ProductCategory.ProductCategoryID = ProductSubCategory.ProductCategoryID
ORDER BY Product.ModifiedDate DESC;
 
SELECT
	Product.ProductID,
	Product.ProductName,
	Product.ProductNumber,
	CostMeasure.UnitMeasureCode,
	CostMeasure.Name AS CostMeasureName,
	ProductVendor.AverageLeadTime,
	ProductVendor.StandardPrice,
	ProductReview.ReviewerName,
	ProductReview.Rating,
	Product.ProductCategory,
	Product.ProductSubCategory
FROM #Product Product
INNER JOIN Production.ProductModel
ON ProductModel.ProductModelID = Product.ProductModelID
LEFT JOIN Production.ProductReview
ON ProductReview.ProductID = Product.ProductID
LEFT JOIN Purchasing.ProductVendor
ON ProductVendor.ProductID = Product.ProductID
LEFT JOIN Production.UnitMeasure CostMeasure
ON ProductVendor.UnitMeasureCode = CostMeasure.UnitMeasureCode;
 
DROP TABLE #Product;
SET STATISTICS TIME OFF;  
GO  