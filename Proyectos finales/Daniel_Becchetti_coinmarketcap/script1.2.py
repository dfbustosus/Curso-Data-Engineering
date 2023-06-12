from requests import Request, Session
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects
import json
from sqlalchemy import create_engine 
import psycopg2
import pandas as pd
from mailjet_rest import Client

# API de CoinMarketCap

url = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest"
parameters = {
  "start":"1",
  "limit":"50",
  "convert":"USD"
}
headers = {
  "Accepts": "application/json",
  "X-CMC_PRO_API_KEY": "391df7d3-b496-471b-867c-25603fc58ac9",
}

session = Session()
session.headers.update(headers)

try:
  response = session.get(url, params=parameters)
  data = json.loads(response.text) 
except (ConnectionError, Timeout, TooManyRedirects) as e:
  print(e)

# DATA

data_filtered = []
for crypto in data["data"]:
  data_filtered.append({
  "name": crypto["name"],
  "symbol": crypto["symbol"],
  "price": crypto["quote"]["USD"]["price"],
  "market_cap": crypto["quote"]["USD"]["market_cap"],
  "percent_change_1h": crypto["quote"]["USD"]["percent_change_1h"],
  "percent_change_24h": crypto["quote"]["USD"]["percent_change_24h"],
  "percent_change_7d": crypto["quote"]["USD"]["percent_change_7d"],
  "volume_24h": crypto["quote"]["USD"]["volume_24h"],
  "circulating_supply": crypto["circulating_supply"],
  "total_supply": crypto["total_supply"],
  "max_supply": crypto["max_supply"]
})

# DataFrame 

df = pd.DataFrame(data_filtered)

# Aplicar Pandas

# Se filtran cryptos cuyo crecimiento en las ultimas 24hrs haya sido mayor a 0.5% 
df_filtered = df[df["percent_change_24h"] > 0.5] 
print(df_filtered)

with open("config.json") as json_file:
    thresholds = json.load(json_file)

mailjet_api_key = "8a554c84dab63e562d74f1cd8d3e210f"

mailjet = Client(auth=(mailjet_api_key))

for crypto in df_filtered.itertuples():
    symbol = crypto.symbol
    price = crypto.price
    if symbol in thresholds["stocks"]:
        crypto_thresholds = thresholds["thresholds"][symbol]
        min_threshold = crypto_thresholds["min"]
        max_threshold = crypto_thresholds["max"]
        if price < min_threshold or price > max_threshold:
            email_subject = f"Alerta!: El limite de {symbol} fue alcanzado"
            email_text = f"El limite de {symbol} fue alcanzado.\nPrecio actual: {price}\nMinimo: {min_threshold}\nMaximo: {max_threshold}"
            data = {
                "Mensaje": [
                    {
                        "De": {
                            "Email": "",
                            "Nombre": ""
                        },
                        "A": [
                            {
                                "Email": "",
                                "Nombre": ""
                            }
                        ],
                        "Subject": email_subject,
                        "Texto": email_text
                    }
                ]
            }
            response = mailjet.send.create(data=data)
            print(response.status_code)
            print(response.json())

# Conexión a DB Redshift

conn = create_engine ("postgresql://becc_daniel_92_coderhouse:b13XV0q9I1iV@data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com:5439/data-engineer-database")
