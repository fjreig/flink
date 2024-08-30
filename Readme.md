# PyFLINK

## INFO

* https://github.com/augustodn/pyflink-docker/tree/main
* https://www.linkedin.com/pulse/how-i-dockerized-apache-flink-kafka-postgresql-data-de-nevrezé-mh8wf
* https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-mongodb/1.1.0-1.18/
* https://medium.com/@dmitri.mahayana/idx-stock-real-time-data-prediction-with-flink-kafka-mongodb-526c6abf291f

### Kafka

```
docker compose up -d redpanda console
```

### Flink

```
docker compose up -d flink-jobmanager flink-taskmanager
```

### Postgres

```
docker compose up -d postgres
```

```
CREATE TABLE raw_fv_data (
    message_id varchar,
    planta_id varchar,
    message varchar,
    timestamp varchar
);
CREATE TABLE alert_fv_data (
    timestamp varchar,
    planta_id varchar,
    Radiacion varchar, 
    Potencia varchar,
    Alerta varchar
);
CREATE TABLE sales_euros (
    seller_id VARCHAR,
    window_start TIMESTAMP(3),
    window_end TIMESTAMP(3),
    window_sales FLOAT
);
```

# mongo

docker compose up -d mongo


# 1. Acceder a flink

```
docker exec -it jobmanager /bin/bash
docker exec -it taskmanager /bin/bash
```

## 1. DataStream

### 1.1. kafka Producer

```
python /opt/flink/usr_jobs/stream/kafka_producer.py
```

### 1.2. kafka Consumer

```
python /opt/flink/usr_jobs/stream/kafka_consumer.py
```

### 1.3. kafka Sink

```
flink run -py /opt/flink/usr_jobs/stream/kafka_sink.py
```

### 1.4. Postgres Sink

```
flink run -py /opt/flink/usr_jobs/stream/postgres_sink.py
```

### 1.5. Postgres Sink Alerts

```
flink run -py /opt/flink/usr_jobs/stream/postgres_sink_alert.py
```

## 2. Tablas

### 2.1. kafka Producer

```
python /opt/flink/usr_jobs/tables/kafka_producer.py
```

### 2.2. kafka Sink

```
flink run -py /opt/flink/usr_jobs/tables/kafka_sink.py
```

### 2.4. postgres Sink

```
flink run -py /opt/flink/usr_jobs/tables/postgres_sink.py
```

### 2.3. mongo Sink

```
flink run -py /opt/flink/usr_jobs/tables/mongo_sink.py
```
