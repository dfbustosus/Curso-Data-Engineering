import requests
import json
# 1. Definir la url de destino
url = "https://data.messari.io/api/v1/assets/bitcoin/metrics"
headers = {"Accept-Encoding": "gzip, deflate"}
# 2. Hacer el response 
response = requests.get(url, headers=headers)
print(response)
# 3. Obtener el json con al data
data = response.json()
print(data.keys())
# 4. Extraer la fecha
fecha=data['status']['timestamp']
print(fecha)
# 5. Obtener los otros datos: volume_last_24_hours, open, low, close y high
volume_last_24_hours=data['data']['market_data']['volume_last_24_hours']
open=data['data']['market_data']['ohlcv_last_1_hour']['open']
low=data['data']['market_data']['ohlcv_last_1_hour']['low']
close=data['data']['market_data']['ohlcv_last_1_hour']['close']
high=data['data']['market_data']['ohlcv_last_1_hour']['high']
# 6. Crear dict final
dict_final={'fecha':fecha, 'volume_last_24_housr': volume_last_24_hours, 'open':open, 'low':low, 'close':close, 'high':high}
# 6.1 Ojo con la fecha modificarla a str si quieren guardar
dict_final['fecha'] = str(dict_final['fecha'])  
print(dict_final)