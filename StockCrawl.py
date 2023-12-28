import os
import time
import shutil
import logging
import requests
import datetime
from pandas import DataFrame
from requests.exceptions import RequestException
os.environ['HTTP_PROXY'] = 'http://10.16.42.23:7890'
os.environ['HTTPS_PROXY'] = 'http://10.16.42.23:7890'


class Stock:
    def __init__(self, crawl_time):
        self.crawl_time = crawl_time
        self.time = str(datetime.datetime.now()).split(' ')[0] + '_' + str(crawl_time) + 'th'
        self.logger = logging.getLogger(__name__)
        self.tmp_file_dir = './tmp'
        self.log_path = '/run_stock_crawl_log_{}.log'.format(self.time)
        self.data_path = f'/stock_{self.time}.txt'
        self.check_path()
        self.set_logger()

    def check_path(self):
        if not os.path.exists(self.tmp_file_dir):
            os.makedirs(self.tmp_file_dir)

    def set_logger(self):
        self.logger.setLevel(level=logging.INFO)
        handler = logging.FileHandler(self.tmp_file_dir + self.log_path)
        handler.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)

    def get_html(self, url, page):
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

    def clear(self):
        for filename in os.listdir(self.tmp_file_dir):
            file_path = os.path.join(self.tmp_file_dir, filename)
            try:
                if os.path.isfile(file_path) or os.path.islink(file_path):
                    os.unlink(file_path)
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)
                self.logger.info('Finish remove tmp file in {}'.format(file_path))
            except Exception as e:
                self.logger.error('Failed to delete %s. Reason: %s' % (file_path, e))

    def crawl(self):
        t = str(datetime.datetime.now()).split(' ')[0] + '_' + str(self.crawl_time) + 'th'
        self.logger.info('The {}th crawl start!'.format(self.crawl_time))
        url = "https://xueqiu.com/service/v5/stock/screener/quote/list"
        stock_hs = []
        page = -1
        while True:
            page += 1
            response = self.get_html(url, page)
            # error
            if response is None:
                self.logger.error('Response failed in {}th time crawler!'
                                  .format(self.crawl_time))
                time.sleep(60)
                break
            response_json = response.json()
            # finish
            try:
                stock_info = response_json['data']['list']
            except:
                self.logger.info('The {}th time crawler finished at localtime {} total of {} pieces of data crawled!'
                                 .format(1, datetime.datetime.now(), len(stock_hs)))
                continue
            for stock in stock_info:
                stock_detail = {
                    '股票名称': stock['name'],
                    '股票代码': stock['symbol'],
                    '当前价': stock['current'],
                    '涨跌额': stock['chg'],
                    '涨跌幅%': stock['percent'],
                    '年初至今': stock['current_year_percent'],
                    '成交量': stock['volume'],
                    '成交额(元)': stock['amount'],
                    '换手率': stock['turnover_rate'],
                    '市盈率': stock['pe_ttm'],
                    '股息率%': stock['dividend_yield'],
                    '市值(元)': stock['market_capital']
                }
                stock_hs.append(stock_detail)
            self.logger.info('Successfully completed crawl {} stock mess on page of {} at localtime {}'
                             .format(len(stock_info), page, t))
        df = DataFrame(data=stock_hs)
        df.drop_duplicates(inplace=True)
        df.to_csv(self.tmp_file_dir + self.data_path, encoding='utf-8', index=False)
        self.logger.info('The {}th crawl finished after dropping duplicates total {} data crawled!'.format(self.crawl_time, len(df)))