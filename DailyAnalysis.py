import os
import sys
import time
from typing import Iterable, Tuple
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext, RDD

sys.path.append('/opt/module/spark-yarn/python/lib/py4j-0.10.9.7-src.zip')
'''
data format
股票名称,股票代码,当前价,涨跌额,涨跌幅%,年初至今,成交量,成交额(元),换手率,市盈率,股息率%,市值(元),时间
'''


def split_data(line):
    t = line.split(',')
    # temp
    crawl_time = int(time.time())
    stock_name = t[0]
    stock_code = t[1]
    markt = t[1][:2]
    now_price = t[2]
    return [stock_name, stock_code, markt, now_price, crawl_time]


def sort_data(rdd):
    res = sorted(rdd[1], key=lambda x: x[-1], reverse=True)
    return (rdd[1], res)


def get_day_avg_function(rdd):
    price_rdd = rdd.map(lambda x: (x[0], (float(x[3]), 1)))
    price_rdd = price_rdd.reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
    price_result = price_rdd.map(lambda x: (x[0], round(x[1][0] / x[1][1], 3)))
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


if __name__ == '__main__':
    sc = SparkSession.builder.config("spark.driver.host", "localhost").appName('StockDaily').master('local[*]').getOrCreate()
    data = sc.sparkContext.textFile('hdfs://hadoop1:8020/stock/stock_crawl_result/2023-12-29/*')
    data = data.filter(lambda x: '股票名称' not in x)
    filter_data = data.map(split_data).sortBy(lambda x: x[-1])
    #temp
    # filter_data.cache()
    day_avg = get_day_avg_function(filter_data)
    day_avg.collect()
    t_data = filter_data.map(lambda x: (x[0], x[1:]))
    group_data: RDD[Tuple[str, Iterable]] = t_data.groupByKey()
    day_max = get_max_price_function(group_data)
    day_max.collect()