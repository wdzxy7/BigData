import json
import os
import time

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


def send_test(time):
    producer.produce(topic_name, key="key", value=json.dumps({'value': 'aaa,bbb,12,{}'.format(str(time))}))
    producer.flush()


def sort_function(value):
    return int(value.split('_')[2].replace('th.txt', ''))

if __name__ == '__main__':
    topic_name = 'FlinkStock'

    conf = {'bootstrap.servers': 'hadoop1:9092, hadoop2:9092',
            'client.id': socket.gethostname(),
            # 'enable.idempotence': True
            }
    producer = Producer(conf)
    files = os.listdir('../dfs_tmp/2024-01-12')
    files = sorted(files, key=sort_function)
    for path in files:
        with open(os.path.join('../dfs_tmp/2024-01-12', path), 'r') as f:
            for line in f.readlines():
                res = msg_process(line)
                flink_data = json.dumps(res)
                producer.produce(topic_name, key="key", value=flink_data)
                producer.flush()
        time.sleep(4)
        print('successful send {}'.format(path))

