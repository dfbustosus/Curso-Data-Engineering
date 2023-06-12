from sqlalchemy import create_engine
from key import USER, HOST, DBNAME, USER, PASSWORD
import pandas as pd


# Validar y cargar la data
def validate_and_load_to_redshift(df_parameter):
    # Crear un objeto de conexión con la base de datos
    conn = create_engine(
        f'postgresql://{USER}:{PASSWORD}@{HOST}:5439/{DBNAME}')

    # Devuelve los datos de la API que ya fueron cargados a Redshift
    df_redshift = pd.read_sql_table('prueba_1', conn)

    concatenated_DF = pd.concat([df_parameter, df_redshift], axis=0)

    # quita los duplicados si existen
    if concatenated_DF.duplicated().any():
        print('hay duplicados')
        # Si los campos 'symbol' y 'last_updated' tienen duplicados quitar.
        concatenated_DF.drop_duplicates(
            inplace=True, subset=['symbol', 'last_updated'])
        print('duplicados quitados de la data.')
        # Enviar los datos del DataFrame a una tabla en Redshift
        concatenated_DF.to_sql(name='prueba_1', con=conn, index=False,
                                    if_exists='replace', method='multi')
        print('data validada y cargada en la base de datos.')
    else:
        print('No hay duplicados')
        # Enviar los datos del DataFrame a una tabla en Redshift
        concatenated_DF.to_sql(name='prueba_1', con=conn, index=False,
                                    if_exists='replace', method='multi')
        print('data validada y cargada en la base de datos.')
