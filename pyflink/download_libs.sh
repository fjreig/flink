#!/bin/bash

mkdir -p ./lib

curl -o ./lib/flink-connector-jdbc-3.1.2-1.18.jar https://repo1.maven.org/maven2/org/apache/flink/flink-connector-jdbc/3.1.2-1.18/flink-connector-jdbc-3.1.2-1.18.jar
curl -o ./lib/postgresql-42.7.3.jar https://jdbc.postgresql.org/download/postgresql-42.7.3.jar
curl -o ./lib/flink-sql-connector-kafka-3.1.0-1.18.jar https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.1.0-1.18/flink-sql-connector-kafka-3.1.0-1.18.jar
curl -o ./lib/flink-connector-mongodb-1.2.0-1.18.jar https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-mongodb/1.2.0-1.18/flink-connector-mongodb-1.2.0-1.18.jar
curl -o ./lib/bson-5.1.3.jar https://repo1.maven.org/maven2/org/mongodb/bson/5.1.3/bson-5.1.3.jar
curl -o ./lib/mongodb-driver-sync-5.1.3.jar https://repo1.maven.org/maven2/org/mongodb/mongodb-driver-sync/5.1.3/mongodb-driver-sync-5.1.3.jar
curl -o ./lib/mongodb-driver-core-5.1.3.jar  https://repo1.maven.org/maven2/org/mongodb/mongodb-driver-core/5.1.3/mongodb-driver-core-5.1.3.jar

echo "Libs downloaded successfully"
