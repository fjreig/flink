# Apache Flink
### Info

https://github.com/augustodn/pyflink-docker/tree/main
https://www.linkedin.com/pulse/how-i-dockerized-apache-flink-kafka-postgresql-data-de-nevrezé-mh8wf

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
CREATE TABLE raw_sensors_data (
    message_id varchar,
    sensor_id varchar,
    message varchar,
    timestamp varchar
);
```

### mongo

```
docker compose up -d mongo mongo-express
```

## 1. Acceder a flink

```
docker exec -it jobmanager /bin/bash
docker exec -it taskmanager /bin/bash
```

## 2. kafka Producer

```
python /opt/flink/usr_jobs/kafka_producer.py
```

## 3. kafka Consumer

```
python /opt/flink/usr_jobs/kafka_consumer.py
```

## 4. kafka Sink

```
flink run -py /opt/flink/usr_jobs/kafka_sink.py
```

## 5. Postgres Sink

```
flink run -py /opt/flink/usr_jobs/postgres_sink.py
```



