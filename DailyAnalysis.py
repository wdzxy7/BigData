import os
import sys
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext

sys.path.append('/opt/module/spark-yarn/python/lib/py4j-0.10.9.7-src.zip')


def split_data(line):
    t = line.split(',')
    return (t[0], 1)


if __name__ == '__main__':
    sc = SparkSession.builder.appName('StockDaily').getOrCreate()
    data = sc.sparkContext.textFile('hdfs://hadoop1:8020/stock/stock_crawl_result/2023-12-29/*')
    data = data.map(split_data)
    count = data.reduceByKey(lambda x, y: x + y)
    res = count.filter(lambda x: x[1] > 4)
    res.foreach(print)