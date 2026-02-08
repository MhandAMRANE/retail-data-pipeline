FROM python:3.10-slim

WORKDIR /app

COPY ../scripts /app/scripts
COPY ../data /app/data

RUN pip install pandas sqlalchemy psycopg2-binary

CMD ["python", "scripts/clean_data.py"]
