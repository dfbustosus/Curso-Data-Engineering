
import pandas as pd
import psycopg2
import yfinance as yf
url="data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws"
data_base="data-engineer-database"
user="dafbustosus_coderhouse"
with open("C:/Users/Windows/Downloads/pwd_coder.txt",'r') as f:
    pwd= f.read()


try:
    conn = psycopg2.connect(
        host='data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com',
        dbname=data_base,
        user=user,
        password=pwd,
        port='5439'
    )
    print("Connected to Redshift successfully!")
    
    # Do some work with the connection here...
    
except Exception as e:
    print("Unable to connect to Redshift.")
    print(e)

'''SQL de ejemplo: Hacerlo en Redshift primero
NOTA: Asegurarse que la tabla sea la misma que tienen en el dataframe con los nombres de columnas
CREATE TABLE stock_prices (
  O decimal(10,2),
  H decimal(10,2),
  L decimal(10,2),
  C decimal(10,2),
  V bigint,
  D decimal(10,2),
  S decimal(10,2)
);
'''

# Verificar que tienen la tabla creada
cur = conn.cursor()
# Execute a SQL query to select data from a table
cur.execute("SELECT * FROM stock_prices")
# Fetch the results
results = cur.fetchall()
print(results)
# results deberia ser una lista vacia


# Traer la data
goo = yf.Ticker('GOOG')
# sacar la informacion historica de 1 año hacia atras
hist = goo.history(period="1y")
hist['Date']=hist.index
hist=hist.reset_index(drop=True)
hist # tenemos 8 columnas

hist=hist.rename(columns={'Open':'O','High':'H','Low':'L','Close':'C','Volume':'V','Dividends':'D','Stock Splits':'S', 'Date':'Dat'})
hist= hist.drop(columns=['Dat'])
#hist

# Mandar a redshift
from psycopg2.extras import execute_values
cur = conn.cursor()
table_name = 'stock_prices'
columns = ['O', 'H', 'L','C','V','D','S']
values = [tuple(x) for x in hist.to_numpy()]
insert_sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES %s"
cur.execute("BEGIN")
execute_values(cur, insert_sql, values)
cur.execute("COMMIT")
