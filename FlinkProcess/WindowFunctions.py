import numpy as np
from typing import Iterable, Tuple
from pyflink.datastream import ProcessWindowFunction


class FastChangeWindowFunction(ProcessWindowFunction):
    def process(self, key: str, context: ProcessWindowFunction.Context, elements: Iterable):
        elements = list(elements)
        stock_name = elements[0][0]
        stock_code = elements[0][1]
        array = np.array(elements)
        prices = array[:, 2:].astype(float)
        max_price = max(prices, key=lambda x: x[0])
        min_price = min(prices, key=lambda x: x[0])
        if max_price[1] > min_price[1]:
            rate = (max_price[0] - min_price[0]) / min_price[0] * 100
            rate = round(rate, 2)
            if rate > 0.25:
                yield stock_name, stock_code, 'rise', rate
        elif max_price[1] < min_price[1]:
            rate = (max_price[0] - min_price[0]) / max_price[0] * 100
            rate = round(rate, 2)
            if rate > 0.25:
                yield stock_name, stock_code, 'fall', rate
