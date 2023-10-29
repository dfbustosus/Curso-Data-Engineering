# Luigi

Luigi no usa DAGs a diferencia de Apache Airflow. Utiliza dos bloques principales

1. `Task`: una transformación o operación que genera un target (e.g un archivo de salida)

2. `Target`: son los resultados de la tarea y el input para la siguiente tarea

Otros aspectos a tener en cuenta:
- `Tasks` son las unidades básicas de trabajo en el pipeline y se pueden crear dependencias entre ellas similares a los DAGs.
- Los `Target` pueden ser el output final o intermediario usado para alimentar otra tarea
- Las `Tasks` se desarrollan con la clase `Task` de Luigi con 3 metodos esenciales

1. `Requires`: especifica las deoendencias en otras tareas
2. `Run`: logica(operaciones) a realizar
3. `Output`: define el output donde se almacenara el resultado

# Ejemplo sencillo

```bash
cd Luigi
python -m venv luigienv
.\luigienv\Scripts\Activate.ps1
pip install luigi
luigid
```

Puedes ir a `http://localhost:8082` y ver la interfaz gráfica de Luigi

Abrir otro terminal para ejecutar el primer pipeline
```bash
cd Luigi
.\luigienv\Scripts\Activate.ps1
python Pipeline_1.py
```

Deberás ver le resultado de `archivo_1.txt` lleno con el mensaje

# Ejemplo más complicado

```bash
pip install pandas
python Pipeline_2.py
```

