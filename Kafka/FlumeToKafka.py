import time
import json
import socket
from confluent_kafka import Producer, Consumer
from confluent_kafka import KafkaException, KafkaError


def msg_process(msg):
    value = msg.value()
    if value:
        value = value.decode('utf-8')  # 假设消息可采用 utf-8解码
    return {
        'topic': msg.topic(),
        'partition': msg.partition(),
        'offset': msg.offset(),
        'value': value
    }


def get_message():
    try:
        consumer.subscribe([in_topic])  # 订阅主题
        while True:
            msg = consumer.poll(timeout=1)
            if msg is None:
                time.sleep(0.1)
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    print('%% %s [%d] reached end at offset %d\n' %
                          (msg.topic(), msg.partition(), msg.offset()))
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                res = msg_process(msg)
                try:
                    flink_data = json.dumps(res)
                except:
                    flink_data = str(res)
                send_message(flink_data)

    finally:
        consumer.close()


def delivery_callback(err, msg):
    if err:
        print('ERROR: Message failed delivery: {}'.format(err))
    else:
        print("Produced event to topic {topic}: key = {key:12} value = {value:12}".format(
            topic=msg.topic(), key=msg.key().decode('utf-8'), value=msg.value().decode('utf-8')))


def send_message(data):
    global count
    producer.produce(out_topic, key="key", value=data, callback=delivery_callback)
    producer.poll(60)
    producer.flush()


if __name__ == '__main__':
    count = 1
    in_topic = 'FlumeStock'
    out_topic = 'FlinkStock'
    consumer_conf = {'bootstrap.servers': 'hadoop1:9092,hadoop2:9092',
                     'group.id': '1',
                     'enable.auto.commit': 'true',
                     'auto.offset.reset': 'smallest',
                     'partition.assignment.strategy': 'cooperative-sticky',
                     }
    consumer = Consumer(consumer_conf)
    producer_conf = {'bootstrap.servers': 'hadoop1:9092,hadoop2:9092',
                     'client.id': socket.gethostname(),
                     'enable.idempotence': True}
    producer = Producer(producer_conf)
    get_message()
