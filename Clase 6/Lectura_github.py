# Importar librería
import pandas as pd
# Definir url importante que sea formato raw
url = 'https://raw.githubusercontent.com/JJTorresDS/stocks-ds-edu/main/stocks.csv'
# Lectura de archivo
df = pd.read_csv(url, index_col=0)
# Subset de columnas de interés
print(df[['AMZN','MCD','SBUX','GOOG','MSFT']].head(5).round(1))
