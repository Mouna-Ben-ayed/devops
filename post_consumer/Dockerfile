FROM python:3.11-slim

COPY . ./

RUN pip install --no-cache-dir kafka-python google-cloud-bigquery pymongo

CMD ["python", "main.py"]
