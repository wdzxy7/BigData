from pyflink.common import Types
from pyflink.datastream.functions import IN1, IN2
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.datastream import ProcessFunction, RuntimeContext, BroadcastProcessFunction
from pyflink.fn_execution.datastream.window.window_operator import Context
from pyflink.fn_execution.state_impl import SynchronousValueRuntimeState


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


class TurnoverTopNProcessFunction(BroadcastProcessFunction):
    def process_element(self, value: IN1, ctx: ReadOnlyContext):


    def process_broadcast_element(self, value: IN2, ctx: Context):
        broadcast_state = ctx
