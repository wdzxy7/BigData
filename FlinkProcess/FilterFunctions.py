from pyflink.datastream import FilterFunction, RuntimeContext


class ChangeWarnFilterFunction(FilterFunction):

    def __init__(self, warn_threshold, warn_count):
        super(ChangeWarnFilterFunction, self).__init__()
        self.warn_threshold = warn_threshold
        self.warn_count = warn_count

    def filter(self, value):
        if value[-2] > self.warn_count:
            # rise
            if value[-3] == 1:
                rate = (value[-1][-1] - value[-1][0]) / value[-1][0] * 100
                rate = round(rate, 2)
                if rate > self.warn_threshold:
                    return value
            # fall
            elif value[-3] == -1:
                rate = (value[-1][0] - value[-1][-1]) / value[-1][0] * 100
                rate = round(rate, 2)
                if rate > self.warn_threshold:
                    return value


class TurnoverFilterFunction(FilterFunction):
    def filter(self, value):
        if value is not None:
            if len(value[-2]) > 1:
                return value