import sys
import json
import logging
from datetime import datetime

from pyflink.common import WatermarkStrategy, SimpleStringSchema, Duration, Time, Types
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode, CheckpointingMode, \
     ExternalizedCheckpointCleanup, EmbeddedRocksDBStateBackend
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.datastream.window import SlidingEventTimeWindows


def split(line):
    line = json.loads(line.encode('utf-8'))
    line = line['value'].replace('\n', '')
    line = line.split(',')
    timestamp = datetime.fromtimestamp(int(line[-1]))
    line.append(timestamp.strftime('%Y-%m-%d %H:%M:%S'))
    return line


class FirstElementTimestampAssigner(TimestampAssigner):

    def extract_timestamp(self, value, record_timestamp):
        return value[-2]


def set_checkpoint():
    global env
    env.enable_checkpointing(60000)
    env.get_checkpoint_config().set_checkpointing_mode(CheckpointingMode.EXACTLY_ONCE)
    env.get_checkpoint_config().set_min_pause_between_checkpoints(5000)
    env.get_checkpoint_config().set_checkpoint_timeout(180000)
    env.get_checkpoint_config().set_tolerable_checkpoint_failure_number(2)
    env.get_checkpoint_config().set_max_concurrent_checkpoints(1)
    env.get_checkpoint_config().enable_externalized_checkpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    env.get_checkpoint_config().enable_unaligned_checkpoints()
    env.get_checkpoint_config().set_checkpoint_storage_dir('file:////root/BigData/FlinkCheckPoints')


def compute_fast_change(key, stream):
    print(stream)
    print('--------------------------------------')




if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")
    env = StreamExecutionEnvironment.get_execution_environment()
    # config kafka
    env.add_jars('file:///root/jars/flink-sql-connector-kafka-3.0.2-1.18.jar')
    env.add_classpaths('file:///root/jars/flink-sql-connector-kafka-3.0.2-1.18.jar')
    # run model
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    env.set_state_backend(EmbeddedRocksDBStateBackend(True))
    env.set_parallelism(2)
    # checkpoint
    set_checkpoint()
    kafka_source = ((KafkaSource.builder()
                     .set_bootstrap_servers('hadoop1:9092,hadoop2:9092')
                     .set_topics('FlinkStock')
                     .set_group_id('FlinkStock')
                     .set_starting_offsets(KafkaOffsetsInitializer.latest())
                     .set_value_only_deserializer(SimpleStringSchema()))
                    .build())
    data = env.from_source(
                kafka_source,
                WatermarkStrategy
                .for_bounded_out_of_orderness(Duration.of_seconds(10))
                .with_timestamp_assigner(FirstElementTimestampAssigner()),
                source_name='kafka_source')
    data = data.map(split).set_parallelism(10)
    data = (data.map(lambda x: (x[0], x[1], x[2], x[-2]))
            .key_by(lambda x: x[0])
            .window(SlidingEventTimeWindows.of(Time.minutes(1), Time.seconds(30)))
            .allowed_lateness(10000)
            .process(compute_fast_change, output_type=Types.TUPLE([Types.STRING(), Types.STRING(), Types.FLOAT()])))
    env.execute()