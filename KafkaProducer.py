import json
import socket
from confluent_kafka import Consumer, Producer
from confluent_kafka import TopicPartition, KafkaError

# conf = {'bootstrap.servers': 'hadoop1:9092,hadoop2:9092',
#         'client.id': socket.gethostname(),
#         'enable.idempotence': True}
with open('conf/KafkaProducerConf.json', 'r') as f:
    conf = f.readlines()
producer = Producer(dict(conf))
producer.produce('test', key="key", value="value")
producer.flush()