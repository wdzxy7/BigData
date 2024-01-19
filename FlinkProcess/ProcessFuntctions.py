from pyflink.common import Types
from pyflink.datastream.functions import IN1, IN2, KeyedBroadcastProcessFunction, KeyedProcessFunction
from pyflink.datastream.state import ValueStateDescriptor, ListStateDescriptor
from pyflink.datastream import ProcessFunction, RuntimeContext, BroadcastProcessFunction
from pyflink.fn_execution.datastream.window.window_operator import Context
from pyflink.fn_execution.state_impl import SynchronousValueRuntimeState, SynchronousListRuntimeState


class TurnoverProcessFunction(ProcessFunction):
    def __init__(self):
        super(TurnoverProcessFunction, self).__init__()
        self.name = 'TurnoverProcessFunction'
        self.pre_price_state: SynchronousValueRuntimeState = None
        self.total_stock_state: SynchronousValueRuntimeState = None

    def open(self, runtime_context: RuntimeContext):
        super().open(runtime_context)
        descriptor = ValueStateDescriptor('pre_price', Types.INT())
        self.pre_price_state = runtime_context.get_state(descriptor)
        descriptor = ValueStateDescriptor('stock_count', Types.INT())
        self.total_stock_state = runtime_context.get_state(descriptor)

    def close(self):
        super().close()

    def process_element(self, value, ctx: 'ProcessFunction.Context'):
        total_stock = self.total_stock_state.value()
        if total_stock is None:
            pass
        pre_price = self.pre_price_state.value()
        if pre_price is None:
            return value
        mid_price = (value[2] + pre_price) / 2


class TurnoverTopNProcessFunction(KeyedBroadcastProcessFunction):

    def __init__(self, broad_descriptor):
        self.broadcast_state_descriptor = broad_descriptor

    def open(self, runtime_context: RuntimeContext):
        super().open(runtime_context)

    def process_element(self, value: IN1, ctx: KeyedBroadcastProcessFunction.ReadOnlyContext):
        pass

    def process_broadcast_element(self, value: IN2, ctx: KeyedBroadcastProcessFunction.Context):
        broadcast_state = ctx.get_broadcast_state(self.broadcast_state_descriptor)


class Temp(ProcessFunction):

    def __init__(self, n):
        super().__init__()
        self.n = n
        self.topN = []

    def open(self, runtime_context: RuntimeContext):
        super().open(runtime_context)

    def process_element(self, value, ctx: 'ProcessFunction.Context'):
        if len(self.topN) < self.n:
            self.topN.append(value)
        else:
            if value[-1] > self.topN[-1][-1]:
                self.topN[-1] = value
        self.topN.sort(key=lambda x: x[-1], reverse=True)
        yield self.topN
