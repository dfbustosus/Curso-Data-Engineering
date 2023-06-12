from datetime import timedelta, datetime
from email import message
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from psycopg2.extras import execute_values
import requests
import calendar
import psycopg2
import pandas as pd
import os
import smtplib

dag_path = os.getcwd() # Obtenemos el path donde estamos

#---------------------------------------------------------------#
#                Datos para el envio del mail                   #
#---------------------------------------------------------------#

with open(dag_path+'/keys/'+"correo_destinatario.txt",'r') as f:
    destinatario = f.read()
with open(dag_path+'/keys/'+"correo_remitente.txt",'r') as f:
    remitente = f.read()
with open(dag_path+'/keys/'+"gmail.txt",'r') as f:
    pass_gm = f.read()

#---------------------------------------------------------------#
#                Datos para conectarme a la BBDD                #
#---------------------------------------------------------------#

url = "data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com"

with open(dag_path+'/keys/'+"db.txt",'r') as f:
    data_base = f.read()
with open(dag_path+'/keys/'+"user.txt",'r') as f:
    user = f.read()
with open(dag_path+'/keys/'+"pwd.txt",'r') as f:
    pwd = f.read()

redshift_conn = {
    'host': url,
    'username': user,
    'database': data_base,
    'port': '5439',
    'pwd': pwd
}
#---------------------------------------------------------------#

default_args = {
    'owner': 'jorge',
    'start_date': datetime(2023, 6, 5),
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

CT_dolar_dag = DAG(
    dag_id='Cotizaciones_ETL',
    default_args=default_args,
    description='carga las cotizaciones del dolar cada hora',
    schedule_interval="@hourly",
    catchup=False
)

#---------------------------------------------------------------#
#                          FUNCIONES                            #
#---------------------------------------------------------------#

def conexion_redshift(exec_date): # Conectamos  con la BBDD 
    print(f"Conectandose a la BD en la fecha: {exec_date}") 
    url = "data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com"
    try:
        conn = psycopg2.connect(
            host=url,
            dbname=redshift_conn["database"],
            user=redshift_conn["username"],
            password=redshift_conn["pwd"],
            port='5439')
        print(conn)
        print("Connected to Redshift successfully!")
    except Exception as e:
        print("Unable to connect to Redshift.")
        print(e)

def creacion_de_tablas():
    conn = psycopg2.connect(
        host=url,
        dbname=redshift_conn["database"],
        user=redshift_conn["username"],
        password=redshift_conn["pwd"],
        port='5439')
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS jorgeflores2311233_coderhouse.cotizaciones(   
            id INTEGER PRIMARY KEY,
            banco VARCHAR(250),
            compra varchar(50),
            venta varchar(50),
            fecha TIMESTAMP,
            fecha_descarga TIMESTAMP
            );
        """)
        print("Tabla Cotizaciones creada")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS jorgeflores2311233_coderhouse.cot_principales(   
            id INT IDENTITY(1,1) PRIMARY KEY,
            dolar_tipo VARCHAR(250),
            compra VARCHAR(50),
            venta VARCHAR(50),
            fecha TIMESTAMP
            );
        """)
        print("Tabla cot_principales creada")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS jorgeflores2311233_coderhouse.evolucion_oficial(  
            id INT IDENTITY(1,1) PRIMARY KEY,
            anio INT,
            mes INT,
            nombre_mes VARCHAR(50),
            valor FLOAT
            );
        """)
        print("Tabla evolucion_oficial creada")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS jorgeflores2311233_coderhouse.evolucion_blue(  
            id INT IDENTITY(1,1) PRIMARY KEY,
            anio INT,
            mes INT,
            nombre_mes VARCHAR(50),
            valor FLOAT
            );
        """)
        print("Tabla evolucion_blue creada")
        conn.commit()

def traer_info2(banco,id): # Obtenemos los datos de la cotizaciones de los bancos 
    url = f"https://argentina-monetary-quotes-api.up.railway.app/api/{banco}"
    response = requests.get(url)
    data = response.json()
    df = pd.DataFrame(data,index=[0])
    columnas = df.columns.tolist()
    columnas.remove('fecha')
    columnas.append('fecha')
    df = df[columnas]
    df.insert(0,'ID',[id])
    df.insert(1,'banco',[banco])
    return df

def traer_info3(endpoint): # Obtenemos los datos de la cotizaciones principales
    url = f"https://argentina-monetary-quotes-api.up.railway.app{endpoint}"
    response = requests.get(url)
    data = response.json()
    df = pd.DataFrame(data, index=[0])
    columnas = df.columns.tolist()
    columnas.remove('fecha')
    columnas.append('fecha')
    df = df[columnas]
    endpoint_nombre = endpoint.split('/')[-1]  # Obtener el último componente de la URL como nombre del endpoint
    df.insert(0, 'dolar_tipo', [endpoint_nombre])
    return df

def traer_evolucion_dolar_oficial(): # Obtenemos la evolucion del dolar oficial
    url = "https://argentina-monetary-quotes-api.up.railway.app/api/evolucion/dolar/oficial"
    response = requests.get(url)
    data = response.json()
    df = pd.DataFrame(data['meses'])
    columnas = df.columns.tolist()
    # Agregar columna con el nombre del mes 
    df.insert(2,'nombre_mes',df['mes'].astype(int).apply(lambda x: calendar.month_name[x]))
    return df

def traer_evolucion_dolar_Blue(): # Obtenemos la evolucion del dolar oficial
    url = f"https://argentina-monetary-quotes-api.up.railway.app/api/evolucion/dolar/blue"
    response = requests.get(url)
    data = response.json()
    df = pd.DataFrame(data['meses'])
    df.insert(2,'nombre_mes',df['mes'].astype(int).apply(lambda x: calendar.month_name[x]))
    return df

def enviar():    # Envio de mail al finalizar  las tareas
    try:
        x=smtplib.SMTP('smtp.gmail.com',587)
        x.starttls()
        x.login(remitente,pass_gm)
        subject='Tarea realizada'
        body_text='Todas las tablas han sido cargada con exito. Saludos!'
        message='Subject: {}\n\n{}'.format(subject,body_text)
        x.sendmail(remitente,destinatario,message)
        print('Envio de mail exitoso')
    except Exception as exception:
        print(exception)
        print('ERROR..!!')

def extraer_data(exec_date): # Obtenemos todos los datos
    try:
        print(f"Adquiriendo data para la fecha: {exec_date}")
        date = datetime.strptime(exec_date, '%Y-%m-%d %H')
        print("OBTENIENDO DATOS DE LAS COTIZACIONES DE LOS BANCOS")
        bancos = ['bbva', 'piano', 'hipotecario', 'galicia', 'santander', 'ciudad', 'supervielle', 'patagonia', 'comafi', 'nacion', 'bind', 'bancor', 'chaco', 'pampa']
        datos_bancos = []
        cont = 0
        for banco in bancos:
            cont += 1
            data = traer_info2(banco, cont)
            data['fecha_descarga'] = date
            datos_bancos.append(data)
        # convertimos en DF y lo guardamos en la carpeta raw_data

        df_completo = pd.concat(datos_bancos, ignore_index=True)
        nombre_archivo = dag_path + '/raw_data/' + "cot_bancos_" + str(date.strftime('%Y-%m-%d-%H')) + ".csv"
        df_completo.to_csv(nombre_archivo, index=False)
        print("Datos Obtenidos cotizaciones de bancos")
        print("--------------------------------------")

        # Obtener datos de las cotizaciones principales

        print("OBTENIENDO DATOS DE LAS COTIZACIONES PRINCIPALES")
        endpoints = ['/api/dolar/oficial', '/api/dolar/blue', '/api/contadoliqui', '/api/dolar/promedio', '/api/dolar/turista', '/api/dolar/bolsa', '/api/dolar/mayorista']
        datos_cot_principales = []
       
        for endpoint in endpoints:
            data = traer_info3(endpoint)
            #datos_cot_principales['fecha'] = date
            datos_cot_principales.append(data)
            df_extra = pd.concat(datos_cot_principales, ignore_index=True)
        # convertimos en DF y lo guardamos en la carpeta raw_data

        df_extra = pd.concat(datos_cot_principales, ignore_index=True)
        otro_nombre_archivo = dag_path + '/raw_data/' + "cot_principales_" + str(date.strftime('%Y-%m-%d-%H')) + ".csv"
        df_extra.to_csv(otro_nombre_archivo, index=False)
        print("Datos Obtenidos de cotizaciones principales")
        print("--------------------------------------")

        # Obtener evolucion de dolar oficial
        print("OBTENIENDO EVOLUCION DE DOLAR OFICIAL")
        df_evol_oficial = traer_evolucion_dolar_oficial()
        otro_nombre_archivo_2 = dag_path + '/raw_data/' + "evo_dolar_oficial_" + str(date.strftime('%Y-%m-%d-%H')) + ".csv"
        df_evol_oficial.to_csv(otro_nombre_archivo_2, index=False)
        print("Evolucion del dolar oficial obtenida")
        print("--------------------------------------")

        # Obtener evolucion de dolar BLUE
        print("OBTENIENDO EVOLUCION DE DOLAR BLUE")
        df_evol_blue = traer_evolucion_dolar_Blue()
        otro_nombre_archivo_3 = dag_path + '/raw_data/' + "evo_dolar_BLUE_" + str(date.strftime('%Y-%m-%d-%H')) + ".csv"
        df_evol_blue.to_csv(otro_nombre_archivo_3, index=False)
        print("Evolucion del dolar blue obtenida")
        print("--------------------------------------")

    except ValueError as e:
        print("Formato datetime debería ser %Y-%m-%d %H", e)
        raise e

def cargar_datos(exec_date): # Cargamos los datos
    print(f"Cargando los datos para la fecha: {exec_date}")
    date = datetime.strptime(exec_date, '%Y-%m-%d %H')
    
    conn = psycopg2.connect(
        host=url,
        dbname=redshift_conn["database"],
        user=redshift_conn["username"],
        password=redshift_conn["pwd"],
        port='5439')
    cur = conn.cursor()

     # Cargar datos de BANCOS principales
    cot_bancos_file = "cot_bancos_" + str(date.strftime('%Y-%m-%d-%H')) + ".csv"
    cot_bancos_records = pd.read_csv(dag_path + '/raw_data/' + cot_bancos_file)
    cot_bancos_tabla = 'cotizaciones'
    cot_bancos_columns = ["id", "banco", "compra", "venta", "fecha", "fecha_descarga"]
    cot_bancos_values = [tuple(x) for x in cot_bancos_records.to_numpy()]
    cur.execute(f"DELETE FROM {cot_bancos_tabla}")
    print("DATOS BORRADOS")
    cot_bancos_insert_sql = f"INSERT INTO {cot_bancos_tabla} ({', '.join(cot_bancos_columns)}) VALUES %s"
    cur.execute("BEGIN")
    execute_values(cur, cot_bancos_insert_sql, cot_bancos_values)
    cur.execute("COMMIT")
    print("DATOS DE BANCOS PRINCIPALES CARGADOS CON ÉXITO")

      # Cargar datos de cotizaciones principales
    cot_principales_file = "cot_principales_" + str(date.strftime('%Y-%m-%d-%H')) + ".csv"
    cot_principales_records = pd.read_csv(dag_path + '/raw_data/' + cot_principales_file)
    cot_principales_tabla = 'cot_principales'
    cot_principales_columns = ['dolar_tipo', 'compra', 'venta', 'fecha']
    cot_principales_values = [tuple(x) for x in cot_principales_records.to_numpy()]
    cur.execute(f"DELETE FROM {cot_principales_tabla}")
    print("DATOS BORRADOS")
    cot_principales_insert_sql = f"INSERT INTO {cot_principales_tabla} ({', '.join(cot_principales_columns)}) VALUES %s"
    cur.execute("BEGIN")
    execute_values(cur, cot_principales_insert_sql, cot_principales_values)
    cur.execute("COMMIT")
    print("DATOS DE COTIZACIONES PRINCIPALES CARGADOS CON ÉXITO")

    # Cargar datos de evolución del dolar oficial
    evolucion_dolar_oficial_file = "evo_dolar_oficial_" + str(date.strftime('%Y-%m-%d-%H')) + ".csv"
    evolucion_dolar_oficial_records = pd.read_csv(dag_path + '/raw_data/' + evolucion_dolar_oficial_file)
    evolucion_dolar_oficial_tabla = 'evolucion_oficial'
    evolucion_dolar_oficial_columns = ['anio', 'mes', 'nombre_mes', 'valor']
    evolucion_dolar_oficial_values = [tuple(x) for x in evolucion_dolar_oficial_records.to_numpy()]
    cur.execute(f"DELETE FROM {evolucion_dolar_oficial_tabla}")
    print("DATOS BORRADOS")
    evolucion_dolar_oficial_insert_sql = f"INSERT INTO {evolucion_dolar_oficial_tabla} ({', '.join(evolucion_dolar_oficial_columns)}) VALUES %s"
    cur.execute("BEGIN")
    execute_values(cur, evolucion_dolar_oficial_insert_sql, evolucion_dolar_oficial_values)
    cur.execute("COMMIT")
    print("DATOS DE EVOLUCIÓN DEL DOLAR OFICIAL CARGADOS CON ÉXITO")

    # Cargar datos de evolución del dolar blue
    evolucion_dolar_blue_file = "evo_dolar_BLUE_" + str(date.strftime('%Y-%m-%d-%H')) + ".csv"
    evolucion_dolar_blue_records = pd.read_csv(dag_path + '/raw_data/' + evolucion_dolar_blue_file)
    evolucion_dolar_blue_tabla = 'evolucion_blue'
    evolucion_dolar_blue_columns = ['anio', 'mes', 'nombre_mes', 'valor']
    evolucion_dolar_blue_values = [tuple(x) for x in evolucion_dolar_blue_records.to_numpy()]
    cur.execute(f"DELETE FROM {evolucion_dolar_blue_tabla}")
    print("DATOS BORRADOS")
    evolucion_dolar_blue_insert_sql = f"INSERT INTO {evolucion_dolar_blue_tabla} ({', '.join(evolucion_dolar_blue_columns)}) VALUES %s"
    cur.execute("BEGIN")
    execute_values(cur, evolucion_dolar_blue_insert_sql, evolucion_dolar_blue_values)
    cur.execute("COMMIT")
    print("DATOS DE EVOLUCIÓN DEL DOLAR BLUE CARGADOS CON ÉXITO")

    cur.close()
    conn.close()

#---------------------------------------------------------------#
#                            TAREAS                             #
#---------------------------------------------------------------#

task_1 = PythonOperator(
    task_id="conexion_BD",
    python_callable=conexion_redshift,
    op_args=["{{ ds }} {{ execution_date.hour }}"],
    dag=CT_dolar_dag
)

task_2 = PythonOperator(
    task_id="crear_tabla",
    python_callable=creacion_de_tablas,
    dag=CT_dolar_dag
)

task_3 = PythonOperator(
    task_id='extraer_data',
    python_callable=extraer_data,
    op_args=["{{ ds }} {{ execution_date.hour }}"],
    dag=CT_dolar_dag,
)

task_4 = PythonOperator(
    task_id="cargar_tabla",
    python_callable=cargar_datos,
    op_args=["{{ ds }} {{ execution_date.hour }}"],
    dag=CT_dolar_dag
)

task_5 = PythonOperator(
    task_id="enviar_mail_confirmacion",
    python_callable=enviar,
    dag=CT_dolar_dag
)

task_1 >> task_2 >> task_3 >> task_4 >> task_5
