FROM python:3.11-bookworm

RUN apt-get update && apt-get install -y openjdk-17-jre-headless procps && rm -rf /var/lib/apt/lists/*

WORKDIR /app

RUN pip install --no-cache-dir \
    pandas==2.1.4 \
    numpy==1.24.3 \
    sqlalchemy==2.0.23 \
    psycopg2-binary==2.9.9 \
    python-dotenv==1.0.0 \
    requests==2.31.0 \
    scikit-learn==1.3.2 \
    seaborn==0.13.0 \
    matplotlib==3.8.2 \
    openpyxl==3.1.2 \
    pyarrow==13.0.0 \
    pyspark==3.4.1

COPY . .

ENV PYTHONUNBUFFERED=1
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV SPARK_LOCAL_IP=127.0.0.1
ENV SPARK_LOCAL_HOSTNAME=localhost
ENV PYSPARK_PYTHON=/usr/local/bin/python
ENV PYSPARK_DRIVER_PYTHON=/usr/local/bin/python

CMD ["python", "main.py"]
