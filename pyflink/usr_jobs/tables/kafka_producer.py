import atexit
import json
import logging
import random
import time
import sys
from confluent_kafka import Producer

logging.basicConfig(
  format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
  datefmt='%Y-%m-%d %H:%M:%S',
  level=logging.INFO,
  handlers=[
      logging.FileHandler("sales_producer.log"),
      logging.StreamHandler(sys.stdout)
  ]
)

logger = logging.getLogger()

PLANTAS_FV = ['Cheste', 'Vara', 'Quart', 'Oliva', 'Alicante']

class ProducerCallback:
    def __init__(self, record, log_success=False):
        self.record = record
        self.log_success = log_success

    def __call__(self, err, msg):
        if err:
            logger.error('Error producing record {}'.format(self.record))
        elif self.log_success:
            logger.info('Produced {} to topic {} partition {} offset {}'.format(
                self.record,
                msg.topic(),
                msg.partition(),
                msg.offset()
            ))

def main():
    logger.info('Starting sales producer')
    conf = {
        'bootstrap.servers': 'redpanda:9092',
        'linger.ms': 200,
        'client.id': 'sales-1',
        'partitioner': 'murmur2_random'
    }

    producer = Producer(conf)

    atexit.register(lambda p: p.flush(), producer)

    i = 1
    while True:
        is_tenth = i % 10 == 0

        sales = {
            'planta_id': random.choice(PLANTAS_FV),
            'potencia': random.randrange(0, 100),
            'radiacion': random.randrange(0, 1200),
            'time_ts': int(time.time() * 1000)
        }
        producer.produce(topic='fv_plantas',
                        value=json.dumps(sales),
                        on_delivery=ProducerCallback(sales, log_success=is_tenth))

        if is_tenth:
            producer.poll(1)
            time.sleep(2)
            i = 0

        i += 1

if __name__ == '__main__':
    main()
