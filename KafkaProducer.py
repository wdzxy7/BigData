import json
import os
from confluent_kafka import Producer
import socket


def msg_process(msg):
    value = msg
    return {
        'value': value
    }


def acked(err, msg):
    if err is not None:
        print("Failed to deliver message: %s: %s" % msg.value(), str(err))
    else:
        print("Message produced: %s" % msg.value())


if __name__ == '__main__':
    topic_name = 'FlinkStock'

    conf = {'bootstrap.servers': 'hadoop1:9092, hadoop2:9092',
            'client.id': socket.gethostname(),
            # 'enable.idempotence': True
            }

    producer = Producer(conf)
    count = 0
    for path in os.listdir('dfs_tmp/2024-01-03'):
        with open(os.path.join('dfs_tmp/2024-01-03', path), 'r') as f:
            for line in f.readlines():
                res = msg_process(line)
                flink_data = json.dumps(res)
                producer.produce(topic_name, key="key", value=flink_data)
        producer.flush()
        count += 1
