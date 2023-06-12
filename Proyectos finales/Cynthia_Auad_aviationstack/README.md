# DATA ENGINEERING - CODERHOUSE

## Proyecto Final - Cynthia Auad

Comision 48145

### Objetivo:
Realizar un pipeline en airflow que viva en un contenedor Docker y realice las siguientes tareas:
1.   Conectarse a una API publica y extraer data en un archivo 
2.   Realizar trasnformaciones necesarias usando Pandas.
3.   Crear la tabla en Redshift y cargar la data.
4.   Enviar alertas mediante SMTP. 


### Descripcion de la API:

Conexion a la Api de [AviationStack](https://aviationstack.com) y extraccion de la base de datos en tiempo real (permite extraer 100 resultados, con el usuario gratuito)

### Observaciones:

- se agrega un .env para las credenciales, se muestra una copia con los pass con ***
- el airflow.config tambien fue editado para ocultar el smtp_password 
- la data extraida se guarda en redshfit. Se crea/actualiza en la task de cargar_data 
- se informa por mail la extraccion exitosa

### Imagenes 
#### Airflow

![Getting Started](../Logs%20Final/Airflow-Graph.jpg)

![Getting Started](../Logs%20Final/Airflow-Grid.jpg)

#### Logs
![Getting Started](../Logs%20Final/Log%20task1.jpg)
![Getting Started](../Logs%20Final/Log%20task2.jpg)
![Getting Started](../Logs%20Final/Log%20task3.jpg)
![Getting Started](../Logs%20Final/Log%20task4.jpg)

#### Xcom
![Getting Started](../Logs%20Final/Xcom1.jpg)

#### SMTP
![Getting Started](../Logs%20Final/SMTP%20-%20mail%20recibido.jpg)