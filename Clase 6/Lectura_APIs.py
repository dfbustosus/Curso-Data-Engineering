# Traer archivo
# cd 'Clase 6'
#wget -O cars_clus.csv https://s3-api.us-geo.objectstorage.softlayer.net/cf-courses-data/CognitiveClass/ML0101ENv3/labs/cars_clus.csv
import pandas as pd
# Ponerlo en el entorno de trabajo
filename = 'cars_clus.csv'
#Lectura del archivo
pdf = pd.read_csv(filename)
# Traer propiedades como shape
print("Shape: ", pdf.shape)
# Mostrar primeras 5 filas
print(pdf.head(5))
