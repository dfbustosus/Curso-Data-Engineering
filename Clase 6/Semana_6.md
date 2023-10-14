# Semana 6

## Ejemplo entregable 1

Para correr el script de ejemplo `EjemploEntregable1.ipynb` se debe ejecutar el docker-compose que contiene el contenedor de Pyspark con Jupyter Notebook.


## Docker Compose con Pyspark

```bash
docker-compose -f ./Semana_6/docker-compose.yml up --build
```

> Password o Token del Jupyter Notebook: `coder`

> URL del Jupyter Notebook: [http://localhost:8888/lab?token=coder](http://localhost:8888/lab?token=coder)

## Arhcivo con claves Redshift

Crear un archivo llamado `.env` con las siguientes claves:

```bash
AWS_REDSHIFT_USER=your-user
AWS_REDSHIFT_PASSWORD=your-password
AWS_REDSHIFT_HOST=data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com
AWS_REDSHIFT_PORT=5439
AWS_REDSHIFT_DBNAME=data-engineer-database
AWS_REDSHIFT_SCHEMA=your-schema
```

Y colocarlo en la carpeta `Semana_6/docker_shared_folder/working_dir/`