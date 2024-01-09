import os
import time
import logging
import requests
import datetime
import StockCrawl
import pyarrow.hdfs as hdfs
from requests.exceptions import RequestException


def get_html(url, page):
    params = {
        "page": page, "size": 1000, "order": "desc", "orderby": "percent", "order_by": "symbol", "market": "CN",
        "type": "sh_sz", "_": int(time.time() * 1000)
    }
    try:
        headers = {
             'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36'
        }
        response = requests.get(url, headers=headers, params=params)
        response.encoding = 'utf-8'
        if response.status_code == 200:
            return response
        return None
    except RequestException:
        return None


def set_logger(logger, t):
    log_path = '../run_log'
    if not os.path.exists(log_path):
        os.makedirs(log_path)
    logger.setLevel(level=logging.INFO)
    handler = logging.FileHandler(os.path.join(log_path, 'run_ice_crawl_log_{}.log'
                                               .format(t)))
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)


def create_dir():
    t = str(datetime.datetime.now()).split(' ')[0]
    data_path = '/stock/stock_crawl_result/' + t
    if not dfs.exists(data_path):
        dfs.mkdir(data_path)
    log_path = '/stock/stock_crawl_logs/' + t
    if not dfs.exists(log_path):
        dfs.mkdir(log_path)
    return data_path, log_path


if __name__ == '__main__':
    dfs = hdfs.connect(host='hdfs://hadoop1', port=8020, user='root')
    data_path, log_path = create_dir()
    crawl_time = 1
    exit_time = datetime.datetime.now().replace(hour=15, minute=31, second=0, microsecond=0)
    sleep_time1 = datetime.datetime.now().replace(hour=11, minute=30, second=0, microsecond=0)
    sleep_time2 = datetime.datetime.now().replace(hour=13, minute=00, second=0, microsecond=0)
    while True:
        Stock = StockCrawl.Stock(crawl_time)
        Stock.crawl()
        # with open(Stock.tmp_file_dir + Stock.log_path, 'br')as f:
        #     dfs.upload(log_path + Stock.log_path, f)
        # with open(Stock.tmp_file_dir + Stock.data_path, 'br') as f:
        #     dfs.upload(data_path + Stock.data_path, f)
        # Stock.clear()
        del Stock
        crawl_time += 1
        current_time = datetime.datetime.now()
        # 休盘
        if sleep_time1 <= current_time <= sleep_time2:
            time.sleep(5400)
        # 结束
        if current_time >= exit_time:
            break
        print(1)