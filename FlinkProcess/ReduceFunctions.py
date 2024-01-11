from pyflink.common import Types
from pyflink.datastream import ReduceFunction, RuntimeContext, AggregateFunction
from pyflink.datastream.state import ValueStateDescriptor, AggregatingStateDescriptor
from pyflink.fn_execution.state_impl import SynchronousValueRuntimeState, SynchronousAggregatingRuntimeState


class ChangeAggregateFunction(AggregateFunction):

    def add(self, value, accumulator):
        return accumulator + value

    def get_result(self, accumulator):
        return accumulator

    def merge(self, acc_a, acc_b):
        return acc_a + acc_b

    def create_accumulator(self):
        return 1


class ChangeWarnReduceFunction(ReduceFunction):

    def __init__(self):
        super(ChangeWarnReduceFunction, self).__init__()
        self.pre_change_state: SynchronousValueRuntimeState = None
        self.pre_price_state: SynchronousValueRuntimeState = None
        self.count_state: SynchronousAggregatingRuntimeState = None
        self.agg_fun = None

    def open(self, runtime_context: RuntimeContext):
        super().open(runtime_context)
        self.agg_fun = ChangeAggregateFunction()
        descriptor = ValueStateDescriptor('pre_state', Types.INT())
        self.pre_change_state = runtime_context.get_state(descriptor)
        descriptor = ValueStateDescriptor('pre_price', Types.INT())
        self.pre_price_state = runtime_context.get_state(descriptor)
        descriptor = AggregatingStateDescriptor('count', self.agg_fun, Types.INT())
        self.count_state = runtime_context.get_aggregating_state(descriptor)

    def close(self):
        super().close()

    def reduce(self, x, y):
        # stock_name stock_code now_price rise/fall count price_list
        if y[2] > x[2]:
            if x[3] == 1:
                x[4] += 1
                x[-1].append(y[2])
            else:
                x[4] = 0
                x[-1] = [y[2]]
            x[3] = 1
        if y[2] < x[2]:
            if x[3] == -1:
                x[4] += 1
                x[-1].append(y[2])
            else:
                x[4] = 0
                x[-1] = [y[2]]
            x[3] = -1
        x[2] = y[2]
        return x

    def state_reduce(self, x, y):
        # stock_name stock_code now_price rise/fall count price_list
        pre_change_state = self.pre_change_state.value()
        if pre_change_state is None:
            pre_change_state = 0
        pre_price_state = self.pre_price_state.value()
        if pre_price_state is None:
            pre_price_state = 0
        if y[2] > pre_price_state:
            if pre_change_state == 1:
                self.count_state.add(1)
            else:
                self.pre_change_state.update(1)
                self.count_state.add(self.agg_fun.create_accumulator())
        elif y[2] < pre_price_state:
            if pre_change_state == -1:
                self.count_state.add(1)
            else:
                self.pre_change_state.update(1)
                self.count_state.add(self.agg_fun.create_accumulator())
        self.pre_price_state.update(y[2])
        return [x[0], x[1],  self.pre_price_state.value(), self.pre_change_state.value(), self.count_state.get()]


class TurnoverReduceFunction(ReduceFunction):
    def reduce(self, value1, value2):
        if value1[-1] != value2[-1]:
            value1[-3].append(value2[-3][0])
            value1[-2].append(value2[-2][0])
            value1[-1].append(value2[-1][0])
            return value1