FROM python:3.11-slim-bookworm

# Java is required for PySpark
# RUN apt-get update && apt-get install -y --no-install-recommends \
#     openjdk-17-jdk-headless curl procps \
#     && rm -rf /var/lib/apt/lists/*
RUN echo 'Acquire::Retries "5";' > /etc/apt/apt.conf.d/80retries \
    && apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless curl procps \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="$JAVA_HOME/bin:$PATH"

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py .
COPY products.json .

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]
