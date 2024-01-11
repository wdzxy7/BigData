import sys
import json
import logging

from pyflink.datastream.connectors import DeliveryGuarantee
from pyflink.datastream.formats.json import JsonRowSerializationSchema

from MapFuntions import *
from FilterFunctions import *
from ReduceFunctions import *
from WindowFunctions import *
from pyflink.common import WatermarkStrategy, SimpleStringSchema, Duration, Time, Types, Row
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode, CheckpointingMode, \
    ExternalizedCheckpointCleanup, EmbeddedRocksDBStateBackend, OutputTag
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer, KafkaSink, \
    KafkaRecordSerializationSchema
from pyflink.datastream.window import SlidingEventTimeWindows


def split_data(line):
    line = json.loads(line.encode('utf-8'))
    line = line['value'].replace('\n', '')
    line = line.split(',')
    line[2] = float(line[2])
    line[-1] = int(line[-1])
    return line


class FirstElementTimestampAssigner(TimestampAssigner):

    def extract_timestamp(self, value, record_timestamp):
        line = json.loads(value.encode('utf-8'))
        line = line['value'].replace('\n', '')
        line = line.split(',')
        return int(line[-1])


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


def change_warning():
    value_type_info = Types.ROW_NAMED(
        field_names=["type", 'stock_name', 'stock_code', 'state', 'rate'],
        field_types=[Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(), Types.FLOAT()]
    )
    json_format = JsonRowSerializationSchema.builder().with_type_info(value_type_info).build()
    kafka_sink = (KafkaSink.builder()
                  .set_bootstrap_servers('hadoop2:9092,hadoop3:9092')
                  .set_record_serializer(KafkaRecordSerializationSchema.builder()
                                         .set_topic("FlinkRes")
                                         .set_value_serialization_schema(json_format)
                                         .build())
                  .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                  # exactly once need
                  # .set_transactional_id_prefix('stock-')
                  # .set_property('transaction.timeout.ms', '600000')
                  .build())
    fast_late = OutputTag("fast_change_late")
    fast_change_data = (data.map(lambda x: (x[0], x[1], x[2], x[-1]))
                        .key_by(lambda x: x[0])
                        .window(SlidingEventTimeWindows.of(Time.seconds(30), Time.seconds(5)))
                        .side_output_late_data(fast_late)
                        .process(FastChangeWindowFunction())
                        )
    fast_change_data.get_side_output(fast_late)
    fast_change_data = fast_change_data.map(lambda x: Row(type='change_warn', stock_name=x[0], stock_code=x[1], state=x[2], rate=x[3])
                                            , output_type=value_type_info)
    fast_change_data.print()
    fast_change_data.sink_to(kafka_sink)


def rise_trend():
    # stock_name stock_code now_price rise/fall count price_list
    trend_data = (data.map(lambda x: [x[0], x[1], x[2], 1, 0, [x[2]]])
                  .key_by(lambda x: x[0])
                  .reduce(ChangeWarnReduceFunction()))
    trend_data.filter(ChangeWarnFilterFunction(warn_threshold=0.3, warn_count=4)).print()


def turnover_change():
    # stock_name stock_code stock_price stock_sale_count stock_sale
    turnover_data = (data.map(TurnoverMapFunction())
                     .key_by(lambda x: x[0])
                     .reduce(TurnoverReduceFunction()))
    res = turnover_data.filter(TurnoverFilterFunction())


if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")
    env = StreamExecutionEnvironment.get_execution_environment()
    # config kafka
    env.add_jars('file:///root/jars/flink-sql-connector-kafka-3.0.2-1.18.jar')
    env.add_classpaths('file:///root/jars/flink-sql-connector-kafka-3.0.2-1.18.jar')
    # run model
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    env.set_state_backend(EmbeddedRocksDBStateBackend(True))
    env.set_parallelism(1)
    # checkpoint
    set_checkpoint()
    kafka_source = ((KafkaSource.builder()
                     .set_bootstrap_servers('hadoop1:9092,hadoop2:9092')
                     .set_topics('FlinkStock')
                     .set_group_id('FlinkStock')
                     .set_starting_offsets(KafkaOffsetsInitializer.latest())
                     .set_value_only_deserializer(SimpleStringSchema()))
                    .build())
    data = env.from_source(kafka_source,
                           WatermarkStrategy
                           .for_bounded_out_of_orderness(Duration.of_seconds(2)),
                           source_name='kafka_source')
    data = data.map(split_data).set_parallelism(10)
    # change_warning()
    # rise_trend()
    # turnover_change()
    env.execute()
