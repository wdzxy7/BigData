import time

from confluent_kafka import Consumer
from confluent_kafka import KafkaException, KafkaError

running = True


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


def consume_loop(consumer, topics):
    global running
    try:
        consumer.subscribe(topics)  # 订阅主题
        while running:
            msg = consumer.poll(timeout=1)
            if msg is None:
                time.sleep(0.1)
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    print('%% %s [%d] reached end at offset %d\n' %
                          (msg.topic(), msg.partition(), msg.offset()))
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                res = msg_process(msg)
                try:
                    result = '{' + '"topic": "{topic}", "partition": {partition}, "offset": {offset}, "value": {value}'.format(
                        **res) + '}\n'
                except Exception:
                    result = '{' + '"topic": "{topic}", "partition": {partition}, "offset": {offset}, "value": "{value}"'.format(
                        **res) + '}\n'
                print(result)

    finally:
        # 关闭消费者以提交最后的偏移量
        consumer.close()


if __name__ == '__main__':
    topic_name = 'FlumeStock'

    # 初始化消费者
    conf = {'bootstrap.servers': 'hadoop1:9092,hadoop2:9092',
            'group.id': '1',
            'enable.auto.commit': 'true',
            'auto.offset.reset': 'smallest',
            }

    consumer = Consumer(conf)
    consume_loop(consumer, [topic_name])  # 可以指定多个主题
