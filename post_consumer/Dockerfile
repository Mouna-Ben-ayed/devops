FROM python:3.11-slim

COPY . ./

RUN pip install --no-cache-dir kafka-python google-cloud-bigquery

CMD ["python", "main.py"]
