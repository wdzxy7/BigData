from pyflink.datastream import MapFunction, RuntimeContext


class TurnoverMapFunction(MapFunction):
    def map(self, value):
        stock_name = value[0]
        stock_code = value[1]
        stock_price = float(value[2])
        stock_sale_count = float(value[6])
        stock_sale = value[7]
        change_rate = float(value[8])
        try:
            stock_sale = float(stock_sale)
        except:
            stock_sale = 0
        return [stock_name, stock_code, stock_price, stock_sale_count, stock_sale, change_rate]