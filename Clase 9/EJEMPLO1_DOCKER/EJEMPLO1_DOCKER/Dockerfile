FROM python:3
RUN pip install nltk
RUN  mkdir WORK_REPO
RUN  cd  WORK_REPO
WORKDIR  /WORK_REPO
ADD hola-mundo.py .
CMD ["python", "-u", "hola-mundo.py"]