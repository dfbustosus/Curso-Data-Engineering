/*Explicacion de problematicas
1. Selección de columnas innecesarias: muchas columnas de varias tablas, lo que podría generar una gran cantidad de datos en la respuesta. 

2. LEFT JOINs innecesarios:  LEFT JOINs para unir algunas de las tablas, lo que significa que estás recuperando todos los registros de la tabla de la izquierda, 
incluso si no hay coincidencias en la tabla de la derecha. 
Podríamos considerar utilizar INNER JOINs en su lugar, lo que podría mejorar el rendimiento.

3. Filtrado y ordenación: No se incluyen cláusulas WHERE para filtrar los resultados

4. Uso de funciones en la cláusula SELECT: funciones en la cláusula SELECT, como Product.Name AS ProductName y CostMeasure.Name AS CostMeasureName. 

5. Falta de índices: La eficiencia de las consultas depende en gran medida de los índices adecuados en las tablas. 

6. Estadísticas de tiempo: SET STATISTICS TIME ON para medir el tiempo de ejecución de la consulta. 
Esto es útil para identificar problemas de rendimiento, pero es importante analizar los resultados para identificar las áreas específicas 
que pueden necesitar optimización.

*/

SET STATISTICS TIME ON
GO
SELECT TOP 25
	Product.ProductID,
	Product.Name AS ProductName,
	Product.ProductNumber,
	CostMeasure.UnitMeasureCode,
	CostMeasure.Name AS CostMeasureName,
	ProductVendor.AverageLeadTime,
	ProductVendor.StandardPrice,
	ProductReview.ReviewerName,
	ProductReview.Rating,
	ProductCategory.Name AS CategoryName,
	ProductSubCategory.Name AS SubCategoryName
FROM Production.Product
INNER JOIN Production.ProductSubCategory
ON ProductSubCategory.ProductSubcategoryID = Product.ProductSubcategoryID
INNER JOIN Production.ProductCategory
ON ProductCategory.ProductCategoryID = ProductSubCategory.ProductCategoryID
INNER JOIN Production.UnitMeasure SizeUnitMeasureCode
ON Product.SizeUnitMeasureCode = SizeUnitMeasureCode.UnitMeasureCode
INNER JOIN Production.UnitMeasure WeightUnitMeasureCode
ON Product.WeightUnitMeasureCode = WeightUnitMeasureCode.UnitMeasureCode
INNER JOIN Production.ProductModel
ON ProductModel.ProductModelID = Product.ProductModelID
LEFT JOIN Production.ProductModelIllustration
ON ProductModel.ProductModelID = ProductModelIllustration.ProductModelID
LEFT JOIN Production.ProductModelProductDescriptionCulture
ON ProductModelProductDescriptionCulture.ProductModelID = ProductModel.ProductModelID
LEFT JOIN Production.ProductDescription
ON ProductDescription.ProductDescriptionID = ProductModelProductDescriptionCulture.ProductDescriptionID
LEFT JOIN Production.ProductReview
ON ProductReview.ProductID = Product.ProductID
LEFT JOIN Purchasing.ProductVendor
ON ProductVendor.ProductID = Product.ProductID
LEFT JOIN Production.UnitMeasure CostMeasure
ON ProductVendor.UnitMeasureCode = CostMeasure.UnitMeasureCode
ORDER BY Product.ProductID DESC;
SET STATISTICS TIME OFF;  
GO  