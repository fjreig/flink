import json
import random
import time
from datetime import datetime, timezone
from typing import Any
import uuid

from kafka import KafkaProducer

SLEEP_TIME = 0.5

def generate_sensor_data() -> dict[str, Any]:
    """Generates random ph plant data. It also adds a timestamp for traceability."""
    planta_id = random.randint(1, 34)
    Radiacion = round(random.uniform(0, 1250), 2)
    Potencia = round(random.uniform(0, 300), 2)
    sensor_data = {
        "message_id": uuid.uuid4().hex,
        "planta_id": planta_id,
        "message": {
            "Radiacion": Radiacion,
            "Potencia": Potencia
        },
        # utc timestamp
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    return sensor_data

def main() -> None:
    producer = KafkaProducer(
        bootstrap_servers="redpanda:9092",
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    while True:
        sensor_data = generate_sensor_data()
        producer.send("FV", value=sensor_data)
        print(f"Produced: {sensor_data}")
        time.sleep(SLEEP_TIME)

if __name__ == "__main__":
    main()