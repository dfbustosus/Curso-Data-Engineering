FROM python:3.9

RUN  mkdir proyecto_coderhouse
RUN  cd  proyecto_coderhouse

WORKDIR  /proyecto_coderhouse


RUN pip install requests  
RUN pip install pandas
RUN pip install psycopg2
RUN pip install python-dotenv
RUN pip install "apache-airflow==2.6.1" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.6.1/constraints-3.9.txt"

ADD proyecto_coderhouse.py .
ADD .env .

CMD ["python", "./proyecto_coderhouse.py"]