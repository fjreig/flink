import json
import logging
from datetime import datetime
import os

from pyflink.common import Row, Types, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import JdbcSink
from pyflink.datastream.connectors.jdbc import (
    JdbcConnectionOptions,
    JdbcExecutionOptions,
)
from pyflink.datastream.connectors.kafka import (
    DeliveryGuarantee,
    KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema,
    KafkaSink,
    KafkaSource,
)

KAFKA_HOST = "redpanda:9092"
POSTGRES_HOST = "postgres:5432"
POSTGRES_DB = os.environ['POSTGRES_DB']

def parse_and_filter(value: str) -> Row | None:
    Radiacion_THRESHOLD = 200.0
    Potencia_THRESHOLD = 50.0
    data = json.loads(value)
    message_id = data["message_id"]
    planta_id = data["planta_id"]
    Radiacion = data["message"]["Radiacion"]
    Potencia = data["message"]["Potencia"]
    timestamp = datetime.strptime(data["timestamp"], "%Y-%m-%dT%H:%M:%S.%f+00:00")
    Alerta = "Sin Generacion"
    if ((Radiacion > Radiacion_THRESHOLD) & (Potencia < Potencia_THRESHOLD)):
        return Row(timestamp, planta_id, Radiacion, Potencia, Alerta)
    return None

def initialize_env() -> StreamExecutionEnvironment:
    """Makes stream execution environment initialization"""
    env = StreamExecutionEnvironment.get_execution_environment()

    # Adding the jar to the flink streaming environment
    env.add_jars(
        f"file:///opt/flink/lib/flink-connector-jdbc-3.1.2-1.18.jar",
        f"file:///opt/flink/lib/postgresql-42.7.3.jar",
        f"file:///opt/flink/lib/flink-sql-connector-kafka-3.1.0-1.18.jar",
    )
    return env

def configure_source(server: str, earliest: bool = False) -> KafkaSource:
    """Makes kafka source initialization"""
    properties = {
        "bootstrap.servers": server,
        "group.id": "iot-sensors",
    }

    offset = KafkaOffsetsInitializer.latest()
    if earliest:
        offset = KafkaOffsetsInitializer.earliest()

    kafka_source = (
        KafkaSource.builder()
        .set_topics("FV")
        .set_properties(properties)
        .set_starting_offsets(offset)
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )
    return kafka_source

def configure_postgre_sink(sql_dml: str, type_info: Types) -> JdbcSink:
    """Makes postgres sink initialization. Config params are set in this function."""
    return JdbcSink.sink(
        sql_dml,
        type_info,
        JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
        .with_url(f"jdbc:postgresql://{POSTGRES_HOST}/{POSTGRES_DB}")
        .with_driver_name("org.postgresql.Driver")
        .with_user_name(os.environ['POSTGRES_USER'])
        .with_password(os.environ['POSTGRES_PASSWORD'])
        .build(),
        JdbcExecutionOptions.builder()
        .with_batch_interval_ms(1000)
        .with_batch_size(200)
        .with_max_retries(5)
        .build(),
    )

def main() -> None:
    """Main flow controller"""
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    logger.addHandler(logging.StreamHandler())

    # Initialize environment
    logger.info("Initializing environment")
    env = initialize_env()

    # Define source and sinks
    logger.info("Configuring source and sinks")
    kafka_source = configure_source(KAFKA_HOST)
    sql_dml = (
        "INSERT INTO alert_fv_data (timestamp, planta_id, Radiacion, Potencia, Alerta) "
        "VALUES (?, ?, ?, ?, ?)"
    )

    TYPE_INFO = Types.ROW(
        [
            Types.SQL_TIMESTAMP(),  # timestamp
            Types.INT(),  # planta_id
            Types.FLOAT(),  # Radiacion
            Types.FLOAT(),  # Potencia
            Types.STRING(),  # Alerta
        ]
    )
    jdbc_sink = configure_postgre_sink(sql_dml, TYPE_INFO)
    logger.info("Source and sinks initialized")

    # Create a DataStream from the Kafka source and assign watermarks
    data_stream = env.from_source(kafka_source, WatermarkStrategy.no_watermarks(), "Kafka FV topic")

    # Make transformations to the data stream
    alarms_data = data_stream.map(parse_and_filter, output_type=TYPE_INFO).filter(lambda x: x is not None)
    logger.info("Defined transformations to data stream")

    logger.info("Ready to sink data")
    alarms_data.print()
    alarms_data.add_sink(jdbc_sink)

    # Execute the Flink job
    env.execute("Flink PostgreSQL Sink Alerts")

if __name__ == "__main__":
    main()
