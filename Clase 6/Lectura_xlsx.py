import pandas as pd
# Lectura de archivo
df= pd.read_excel('defaultoutput.xlsx')
# Mostrar las priemra 5 filas
df.head()
# Elegir columnas de interés
print(df[['index','ID','Year_Birth','Education','Income']].head())
