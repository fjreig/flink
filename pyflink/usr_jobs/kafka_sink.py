import json
import logging
from datetime import datetime

from pyflink.common import Row, Types, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import JdbcSink

from pyflink.datastream.connectors.kafka import (
    DeliveryGuarantee,
    KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema,
    KafkaSink,
    KafkaSource,
)

KAFKA_HOST = "redpanda:9092"

def parse_data(data: str) -> Row:
    data = json.loads(data)
    message_id = data["message_id"]
    sensor_id = int(data["sensor_id"])
    message = json.dumps(data["message"])
    timestamp = datetime.strptime(data["timestamp"], "%Y-%m-%dT%H:%M:%S.%f+00:00")
    return Row(message_id, sensor_id, message, timestamp)

def parse_and_filter(value: str) -> str | None:
    Radiacion_THRESHOLD = 200.0
    Potencia_THRESHOLD = 50.0
    data = json.loads(value)
    message_id = data["message_id"]
    planta_id = data["planta_id"]
    Radiacion = data["message"]["Radiacion"]
    Potencia = data["message"]["Potencia"]
    timestamp = data["timestamp"]
    if ((Radiacion > Radiacion_THRESHOLD) & (Potencia < Potencia_THRESHOLD)):
        alert_message = {
            "message_id": message_id,
            "planta_id": planta_id,
            "Radiacion": Radiacion,
            "Potencia": Potencia,
            "alert": "Sin Generacion",
            "timestamp": timestamp
        }
        return json.dumps(alert_message)
    return None

def initialize_env() -> StreamExecutionEnvironment:
    """Makes stream execution environment initialization"""
    env = StreamExecutionEnvironment.get_execution_environment()

    # Adding the jar to the flink streaming environment
    env.add_jars(
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

def configure_kafka_sink(server: str, topic_name: str) -> KafkaSink:
    return (
        KafkaSink.builder()
        .set_bootstrap_servers(server)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(topic_name)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .build()
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
    kafka_sink = configure_kafka_sink(KAFKA_HOST, "FV_alerts")
    logger.info("Source and sinks initialized")

    # Create a DataStream from the Kafka source and assign watermarks
    data_stream = env.from_source(
        kafka_source, WatermarkStrategy.no_watermarks(), "Kafka sensors topic"
    )

    # Make transformations to the data stream
    alarms_data = data_stream.map(parse_and_filter, output_type=Types.STRING()).filter(lambda x: x is not None)
    logger.info("Defined transformations to data stream")

    logger.info("Ready to sink data")
    alarms_data.print()
    alarms_data.sink_to(kafka_sink)

    # Execute the Flink job
    env.execute("Flink Kafka Sink")

if __name__ == "__main__":
    main()
