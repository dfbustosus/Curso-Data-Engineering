import requests 
from PIL import Image
from io import BytesIO
# 1. URL de destino
url = "https://api.imgflip.com/get_memes"
headers = {"Accept-Encoding": "gzip, deflate"}
# 2. Obtener response
response = requests.get(url, headers=headers)
data = response.json()
# 3. Verificar keys de data
print(data.keys())
# 4. Mirar estructura y consistencia de datos
print(len(data['data']['memes']))
print(data['data']['memes'][3])
#5. Obtener figura aleatoria
destino= data['data']['memes'][3]
response = requests.get(destino['url'])
img = Image.open(BytesIO(response.content))
img.save("imagen_meme.jpeg", "JPEG")
print('Proceso finalizado con exito')