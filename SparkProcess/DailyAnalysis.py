import sys
import time
import datetime
from typing import Iterable, Tuple
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext, RDD
from pyspark.sql.functions import lit, split, expr, col

sys.path.append('/opt/module/spark-yarn/python/lib/py4j-0.10.9.7-src.zip')
'''
data format
股票名称,股票代码,当前价,涨跌额,涨跌幅%,年初至今,成交量,成交额(元),换手率,市盈率,股息率%,市值(元),时间
'''


def get_day_avg_function(rdd):
    price_rdd = rdd.map(lambda x: (x[0], (x[1], x[3], 1)))
    price_rdd: RDD[Tuple[str, Iterable]] = price_rdd.reduceByKey(lambda x, y: (x[0], x[1] + y[1], x[2] + y[2]))
    price_result = price_rdd.map(lambda x: (x[0], x[1][0], round(x[1][1] / x[1][2], 3)))
    return price_result


def get_max_price_function(rdd):
    def get_max(t):
        t_max = -1
        for i in t[1]:
            if float(i[-2]) > t_max:
                t_max = float(i[-2])
        return t[0], t_max
    max_price_rdd = rdd.map(get_max)
    return max_price_rdd


def get_min_price_function(rdd):
    def get_min(t):
        t_min = float("inf")
        for i in t[1]:
            if float(i[-2]) < t_min:
                t_min = float(i[-2])
        return t[0], t_min
    min_price_rdd = rdd.map(get_min)
    return min_price_rdd


def get_rise_fall(rdd):
    def get_rise(t):
        price = list(t[1])
        start = price[0][1]
        end = price[-1][1]
        return price[0][0], end - start
    diff_rdd = rdd.map(get_rise)
    return diff_rdd


def cal_rise_rate(rdd):
    rdd = list(rdd)
    diff = round((rdd[-1][0] - rdd[0][0]) / rdd[0][0] * 100, 3)
    return str(diff) + '%'


if __name__ == '__main__':
    sc = (SparkSession.builder.master('local[*]')
          .config("spark.driver.host", "localhost")
          .appName('StockDaily')
          .enableHiveSupport()
          .getOrCreate())
    day = str(datetime.datetime.now()).split(' ')[0]
    raw_data = sc.sparkContext.textFile('hdfs://hadoop1:8020/stock/stock_crawl_result/{}/*'.format(day))
    filter_data = raw_data.map(lambda x: x.split(',')).sortBy(lambda x: x[-1])
    # name, code, market, price, time
    # day avg
    price_data = filter_data.map(lambda x: (x[0], x[1], x[1][:2], float(x[2]), x[-1]))
    day_avg = get_day_avg_function(price_data)
    day_avg_res = day_avg

    # day max
    t_data = price_data.map(lambda x: (x[0], x[1:]))
    group_data = t_data.groupByKey()
    day_max = get_max_price_function(group_data)
    day_max_res = day_max
    # day min
    day_min = get_min_price_function(group_data)
    # rising count falling count
    # code, market, price, time
    pure_price_data = price_data.map(lambda x: (x[1], (x[2], x[3], x[-1])))
    diff_price_data = pure_price_data.groupByKey()
    diff_data = get_rise_fall(diff_price_data)
    diff_data = diff_data.groupByKey()
    rise_res = diff_data.mapValues(lambda x: sum(1 for i in x if i > 0))
    const_res = diff_data.mapValues(lambda x: sum(1 for i in x if i == 0))
    fall_res = diff_data.mapValues(lambda x: sum(1 for i in x if i < 0))

    # rise fall rate
    rise_rate = price_data.map(lambda x: (x[0], (x[3], x[-1]))).groupByKey()
    rise_rate_res = rise_rate.mapValues(cal_rise_rate)

    # generate res
    combine_rdd = rise_res.union(const_res)
    combine_rdd = combine_rdd.union(fall_res)
    combine_rdd = combine_rdd.groupByKey().mapValues(lambda x: list(x))
    rise_df = sc.createDataFrame(combine_rdd, ['market', 'temp'])
    split_column = [expr('temp[0]').alias('rise'), expr('temp[1]').alias('const'), expr('temp[2]').alias('fall')]
    rise_df = rise_df.select(col('market'), *split_column).withColumn('Time', lit(day))
    rise_df.show()

    rise_rate_df = sc.createDataFrame(rise_rate_res, ['StockCode', 'Rate'])
    rise_rate_df.withColumn('Time', lit(day))