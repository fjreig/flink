import json

from pyflink.common import WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer, KafkaSource

def parse_and_filter(value: str) -> str | None:
    TEMP_THRESHOLD = 30.0
    data = json.loads(value)
    message_id = data["message_id"]
    sensor_id = data["sensor_id"]
    temperature = data["message"]["temperature"]
    timestamp = data["timestamp"]
    if temperature > TEMP_THRESHOLD:
        alert_message = {
            "message_id": message_id,
            "sensor_id": sensor_id,
            "temperature": temperature,
            "alert": "High temperature detected",
            "timestamp": timestamp
        }
        return json.dumps(alert_message)
    return None

def main() -> None:
    # Create a StreamExecutionEnvironment
    env = StreamExecutionEnvironment.get_execution_environment()

    # Adding the jar to the flink streaming environment
    env.add_jars(
        f"file:///opt/flink/lib/flink-sql-connector-kafka-3.1.0-1.18.jar"
    )

    properties = {"bootstrap.servers": "redpanda:9092","group.id": "iot-sensors",}

    earliest = False
    offset = (
        KafkaOffsetsInitializer.earliest()
        if earliest
        else KafkaOffsetsInitializer.latest()
    )

    # Create a Kafka Source
    kafka_source = (
        KafkaSource.builder()
        .set_topics("sensors")
        .set_properties(properties)
        .set_starting_offsets(offset)
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    # Create a DataStream from the Kafka source and assign watermarks
    data_stream = env.from_source(
        kafka_source, WatermarkStrategy.no_watermarks(), "Kafka sensors topic"
    )

    # Print line for readablity in the console
    print("start reading data from kafka")

    # Filter events with temperature above threshold
    alerts = data_stream.map(parse_and_filter).filter(lambda x: x is not None)

    # Show the alerts in the console
    alerts.print()

    # Execute the Flink pipeline
    env.execute("Kafka Sensor Consumer")

if __name__ == "__main__":
    main()