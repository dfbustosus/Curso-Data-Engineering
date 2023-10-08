# Imagen de python
FROM python:3.9

# Setera el working directory dentro del container
WORKDIR /app

# Copiar el archivo requirements.txt e instalar las dependencias 
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar la app de Flask y la carpeta templates en el container 
COPY app.py .
COPY templates/index.html templates/index.html

# Mostrarle al usuario el puerto donde el puerto se va a exponer 
EXPOSE 8000

# Iniciar la flask app cuando el container se lanze
CMD ["python", "app.py"]
# Para ejecutar !!!!!
# docker build -t flask_david .
# docker run -d -p 8000:8000 flask_david
