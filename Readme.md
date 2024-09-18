# PyFLINK

## Info

* https://github.com/augustodn/pyflink-docker/tree/main
* https://www.linkedin.com/pulse/how-i-dockerized-apache-flink-kafka-postgresql-data-de-nevrezé-mh8wf
* https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-mongodb/1.1.0-1.18/
* https://medium.com/@dmitri.mahayana/idx-stock-real-time-data-prediction-with-flink-kafka-mongodb-526c6abf291f
* https://github.com/diptimanr/kafka_flink_getting_started/tree/master

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
CREATE TABLE fv_table (
    planta_id VARCHAR,
    window_start TIMESTAMP(3),
    window_end TIMESTAMP(3),
    Potencia FLOAT,
    Radiacion FLOAT
);
CREATE TABLE fv_pavasal_Cheste (
    window_start TIMESTAMP(3),
    window_end TIMESTAMP(3),
    Radiacion FLOAT,
    PA FLOAT
);
```

# mongo

docker compose up -d mongo


# 1. Acceder a flink

```
docker exec -it jobmanager /bin/bash
docker exec -it taskmanager /bin/bash
```

```
docker exec -it jobmanager ./bin/sql-client.sh
```

```
docker exec -it jobmanager ./bin/sql-client.sh -f /opt/flink/usr_jobs/sql/Tablas.sql
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

### 2.3. postgres Sink

```
flink run -py /opt/flink/usr_jobs/tables/postgres_sink.py
```

### 2.4. mongo Sink

```
flink run -py /opt/flink/usr_jobs/tables/mongo_sink.py
```

### 2.5. Print Sink

```
python /opt/flink/usr_jobs/tables/print_sink.py
```


## 3. Examples
```
python /opt/flink/usr_jobs/examples/01_batch_csv_process.py
```

```
python /opt/flink/usr_jobs/examples/02_batch_csv_flinksql.py
```

```
python /opt/flink/usr_jobs/examples/04_kafka_flinksql_producer.py
```

```
python /opt/flink/usr_jobs/examples/05_kafka_flinksql_tumbling_window.py
```

```
python /opt/flink/usr_jobs/examples/06_kafka_pyflink_tableapi_tumbling_window.py
```

```
python /opt/flink/usr_jobs/examples/09_pyflink_udf_tableapi.py
```
